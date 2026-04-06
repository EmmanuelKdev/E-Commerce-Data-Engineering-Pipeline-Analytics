"""
dags/02_transform_silver.py
────────────────────────────
DAG 2 — transform_ecommerce_silver

Pipeline:
  1. create_silver_schema     — CREATE SCHEMA silver (idempotent, runs first)
  2. silver_*                 — clean, cast, deduplicate, and order bronze TEXT
                                data into fully typed silver tables
  3. silver_complete          — summary log

Bronze → Silver transformation rules applied to every table:
  • NULLIF(..., '')           — converts empty strings to proper NULLs
  • Explicit type casts       — ::timestamp, ::int, ::numeric, ::date
  • TRIM() / LOWER() / INITCAP() — normalise free-text fields
  • DISTINCT ON / ROW_NUMBER  — deduplicate on natural keys
  • ORDER BY purchased_at / event date — rows sorted chronologically
  • WHERE guards              — drop rows missing mandatory keys
  • Derived / helper columns  — fulfilment_days, delivered_on_time, etc.
  • _loaded_at passthrough    — audit column preserved from bronze

Silver tables are the direct input for DAG 3 (dims + facts).

Schedule: daily at 02:00 UTC  (after bronze ingest at 01:00, before gold at 03:00)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

CONN_ID = "postgres_warehouse_local"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


# ── Utility ───────────────────────────────────────────────────────────────────

def run_sql(label: str, sql: str) -> dict:
    """
    Execute one or more semicolon-separated SQL statements in a single
    transaction. psycopg2 silently ignores everything after the first ';'
    when given a multi-statement string, so we split manually.

    Commits on success; rolls back the entire block on any error.
    """
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                logger.debug(f"[{label}] >>> {stmt[:120]}")
                cur.execute(stmt)
        conn.commit()
        logger.info(f"[{label}] OK — {len(statements)} statement(s) executed")
    except Exception as exc:
        conn.rollback()
        logger.error(f"[{label}] FAILED — rolled back. Error: {exc}")
        raise
    finally:
        conn.close()
    return {"step": label, "status": "ok", "statements": len(statements)}


# ── Schema ────────────────────────────────────────────────────────────────────

CREATE_SILVER_SCHEMA_SQL = """
CREATE SCHEMA IF NOT EXISTS silver
"""

# ── Silver table SQL ──────────────────────────────────────────────────────────
#
# Design principles
# -----------------
#  1. Source is always bronze.* (all columns arrive as TEXT).
#  2. Empty strings → NULL via NULLIF(..., '') before any cast.
#  3. Every meaningful column is cast to its correct type.
#  4. Free-text geographic fields are TRIM()ed and INITCAP()ed for consistency.
#  5. Identifiers (IDs, codes) are TRIM()ed but kept as-is (no case change).
#  6. Monetary / numeric values use numeric(12,2) for financial precision.
#  7. Rows with NULL mandatory keys are excluded (WHERE … IS NOT NULL).
#  8. Duplicates are removed with DISTINCT ON ordered by the best available
#     tiebreak (most recent timestamp, highest seq, etc.).
#  9. Final ORDER BY puts rows in natural chronological / logical order so
#     downstream SELECT * queries return sensible results without extra sorting.
# 10. _loaded_at is passed through from bronze for full audit lineage.

# ── Orders ────────────────────────────────────────────────────────────────────

SILVER_ORDERS_SQL = """
DROP TABLE IF EXISTS silver.orders CASCADE;
CREATE TABLE silver.orders AS
WITH deduped AS (
    SELECT
        TRIM(order_id)                                                  AS order_id,
        TRIM(customer_id)                                               AS customer_id,
        LOWER(TRIM(order_status))                                       AS order_status,
        NULLIF(TRIM(order_purchase_timestamp),  '')::timestamp          AS purchased_at,
        NULLIF(TRIM(order_approved_at),         '')::timestamp          AS approved_at,
        NULLIF(TRIM(order_delivered_carrier_date), '')::timestamp       AS shipped_at,
        NULLIF(TRIM(order_delivered_customer_date), '')::timestamp      AS delivered_at,
        NULLIF(TRIM(order_estimated_delivery_date), '')::timestamp      AS estimated_delivery_at,
        -- Derived: fulfilment duration (NULL when either timestamp missing)
        CASE
            WHEN NULLIF(TRIM(order_delivered_customer_date), '') IS NOT NULL
             AND NULLIF(TRIM(order_purchase_timestamp), '')      IS NOT NULL
            THEN EXTRACT(DAY FROM (
                     NULLIF(TRIM(order_delivered_customer_date), '')::timestamp
                   - NULLIF(TRIM(order_purchase_timestamp), '')::timestamp
                 ))::int
        END                                                             AS fulfilment_days,
        -- Derived: on-time flag (NULL when delivery date unknown)
        CASE
            WHEN NULLIF(TRIM(order_delivered_customer_date), '') IS NOT NULL
             AND NULLIF(TRIM(order_estimated_delivery_date), '') IS NOT NULL
            THEN (
                NULLIF(TRIM(order_delivered_customer_date), '')::timestamp
                <= NULLIF(TRIM(order_estimated_delivery_date), '')::timestamp
            )
        END                                                             AS delivered_on_time,
        _loaded_at::timestamp                                           AS _loaded_at,
        -- Deduplication rank: keep the most recently loaded record per order
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(order_id)
            ORDER BY _loaded_at DESC
        ) AS _rn
    FROM bronze.orders
    WHERE NULLIF(TRIM(order_id), '')     IS NOT NULL
      AND NULLIF(TRIM(customer_id), '')  IS NOT NULL
)
SELECT
    order_id,
    customer_id,
    order_status,
    purchased_at,
    approved_at,
    shipped_at,
    delivered_at,
    estimated_delivery_at,
    fulfilment_days,
    delivered_on_time,
    _loaded_at
FROM deduped
WHERE _rn = 1
ORDER BY purchased_at NULLS LAST, order_id;

ALTER TABLE silver.orders ADD PRIMARY KEY (order_id);
CREATE INDEX idx_silver_orders_customer    ON silver.orders (customer_id);
CREATE INDEX idx_silver_orders_status      ON silver.orders (order_status);
CREATE INDEX idx_silver_orders_purchased   ON silver.orders (purchased_at)
"""

# ── Order Items ───────────────────────────────────────────────────────────────

SILVER_ORDER_ITEMS_SQL = """
DROP TABLE IF EXISTS silver.order_items CASCADE;
CREATE TABLE silver.order_items AS
WITH deduped AS (
    SELECT
        TRIM(order_id)                                              AS order_id,
        NULLIF(TRIM(order_item_id), '')::int                        AS item_seq,
        TRIM(product_id)                                            AS product_id,
        TRIM(seller_id)                                             AS seller_id,
        NULLIF(TRIM(shipping_limit_date), '')::timestamp            AS shipping_limit_at,
        NULLIF(TRIM(price), '')::numeric(12, 2)                     AS unit_price,
        NULLIF(TRIM(freight_value), '')::numeric(12, 2)             AS freight_value,
        _loaded_at::timestamp                                       AS _loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(order_id), NULLIF(TRIM(order_item_id), '')::int
            ORDER BY _loaded_at DESC
        ) AS _rn
    FROM bronze.order_items
    WHERE NULLIF(TRIM(order_id),    '') IS NOT NULL
      AND NULLIF(TRIM(product_id),  '') IS NOT NULL
      AND NULLIF(TRIM(seller_id),   '') IS NOT NULL
      AND NULLIF(TRIM(price),       '') IS NOT NULL      -- items without a price are unusable
)
SELECT
    order_id,
    item_seq,
    product_id,
    seller_id,
    shipping_limit_at,
    unit_price,
    freight_value,
    -- Derived: total line value for convenience
    (unit_price + COALESCE(freight_value, 0))           AS line_total,
    _loaded_at
FROM deduped
WHERE _rn = 1
  AND unit_price > 0                                    -- exclude zero-value / erroneous rows
ORDER BY order_id, item_seq NULLS LAST;

CREATE INDEX idx_silver_order_items_order   ON silver.order_items (order_id);
CREATE INDEX idx_silver_order_items_product ON silver.order_items (product_id);
CREATE INDEX idx_silver_order_items_seller  ON silver.order_items (seller_id)
"""

# ── Customers ─────────────────────────────────────────────────────────────────

SILVER_CUSTOMERS_SQL = """
DROP TABLE IF EXISTS silver.customers CASCADE;
CREATE TABLE silver.customers AS
WITH deduped AS (
    SELECT
        TRIM(customer_id)                                           AS customer_id,
        TRIM(customer_unique_id)                                    AS customer_unique_id,
        TRIM(customer_zip_code_prefix)                              AS zip_code,
        INITCAP(TRIM(customer_city))                                AS city,
        UPPER(TRIM(customer_state))                                 AS state,   -- 2-char BR state code
        _loaded_at::timestamp                                       AS _loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(customer_id)
            ORDER BY _loaded_at DESC
        ) AS _rn
    FROM bronze.customers
    WHERE NULLIF(TRIM(customer_id),        '') IS NOT NULL
      AND NULLIF(TRIM(customer_unique_id), '') IS NOT NULL
)
SELECT
    customer_id,
    customer_unique_id,
    zip_code,
    city,
    state,
    _loaded_at
FROM deduped
WHERE _rn = 1
ORDER BY customer_unique_id, customer_id;

ALTER TABLE silver.customers ADD PRIMARY KEY (customer_id);
CREATE INDEX idx_silver_customers_unique ON silver.customers (customer_unique_id);
CREATE INDEX idx_silver_customers_state  ON silver.customers (state)
"""

# ── Products ──────────────────────────────────────────────────────────────────

SILVER_PRODUCTS_SQL = """
DROP TABLE IF EXISTS silver.products CASCADE;
CREATE TABLE silver.products AS
WITH translated AS (
    -- Join translation table to get English category names
    SELECT
        p.product_id,
        COALESCE(
            NULLIF(TRIM(t.product_category_name_english), ''),
            NULLIF(TRIM(p.product_category_name), ''),
            'unknown'
        )                                                           AS category,
        NULLIF(TRIM(p.product_weight_g),           '')::numeric    AS weight_g,
        NULLIF(TRIM(p.product_length_cm),          '')::numeric    AS length_cm,
        NULLIF(TRIM(p.product_height_cm),          '')::numeric    AS height_cm,
        NULLIF(TRIM(p.product_width_cm),           '')::numeric    AS width_cm,
        NULLIF(TRIM(p.product_photos_qty),         '')::int        AS photos_qty,
        NULLIF(TRIM(p.product_description_lenght), '')::int        AS description_len,
        NULLIF(TRIM(p.product_name_lenght),        '')::int        AS name_len,
        p._loaded_at::timestamp                                    AS _loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(p.product_id)
            ORDER BY p._loaded_at DESC
        ) AS _rn
    FROM bronze.products p
    LEFT JOIN bronze.product_category_translation t
           ON TRIM(p.product_category_name) = TRIM(t.product_category_name)
    WHERE NULLIF(TRIM(p.product_id), '') IS NOT NULL
),
deduped AS (
    SELECT * FROM translated WHERE _rn = 1
)
SELECT
    product_id,
    category,
    weight_g,
    length_cm,
    height_cm,
    width_cm,
    -- Derived: volumetric weight (cm³ → g equivalent, standard logistics formula)
    CASE
        WHEN length_cm IS NOT NULL AND height_cm IS NOT NULL AND width_cm IS NOT NULL
        THEN ROUND((length_cm * height_cm * width_cm) / 5000.0, 2)
    END                                                             AS volumetric_weight_kg,
    photos_qty,
    description_len,
    name_len,
    _loaded_at
FROM deduped
ORDER BY category, product_id;

ALTER TABLE silver.products ADD PRIMARY KEY (product_id);
CREATE INDEX idx_silver_products_category ON silver.products (category)
"""

# ── Sellers ───────────────────────────────────────────────────────────────────

SILVER_SELLERS_SQL = """
DROP TABLE IF EXISTS silver.sellers CASCADE;
CREATE TABLE silver.sellers AS
WITH deduped AS (
    SELECT
        TRIM(seller_id)                                             AS seller_id,
        TRIM(seller_zip_code_prefix)                                AS zip_code,
        INITCAP(TRIM(seller_city))                                  AS city,
        UPPER(TRIM(seller_state))                                   AS state,
        _loaded_at::timestamp                                       AS _loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(seller_id)
            ORDER BY _loaded_at DESC
        ) AS _rn
    FROM bronze.sellers
    WHERE NULLIF(TRIM(seller_id), '') IS NOT NULL
)
SELECT
    seller_id,
    zip_code,
    city,
    state,
    _loaded_at
FROM deduped
WHERE _rn = 1
ORDER BY state, city, seller_id;

ALTER TABLE silver.sellers ADD PRIMARY KEY (seller_id);
CREATE INDEX idx_silver_sellers_state ON silver.sellers (state)
"""

# ── Payments ──────────────────────────────────────────────────────────────────

SILVER_PAYMENTS_SQL = """
DROP TABLE IF EXISTS silver.payments CASCADE;
CREATE TABLE silver.payments AS
WITH deduped AS (
    SELECT
        TRIM(order_id)                                              AS order_id,
        NULLIF(TRIM(payment_sequential), '')::int                   AS payment_seq,
        LOWER(TRIM(payment_type))                                   AS payment_type,
        NULLIF(TRIM(payment_installments), '')::int                 AS installments,
        NULLIF(TRIM(payment_value), '')::numeric(12, 2)             AS amount,
        _loaded_at::timestamp                                       AS _loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(order_id), NULLIF(TRIM(payment_sequential), '')::int
            ORDER BY _loaded_at DESC
        ) AS _rn
    FROM bronze.payments
    WHERE NULLIF(TRIM(order_id),       '') IS NOT NULL
      AND NULLIF(TRIM(payment_value),  '') IS NOT NULL
)
SELECT
    order_id,
    payment_seq,
    payment_type,
    COALESCE(installments, 1)                                       AS installments,  -- default 1 if NULL
    amount,
    _loaded_at
FROM deduped
WHERE _rn   = 1
  AND amount > 0                                                     -- drop zero / refund-only rows
ORDER BY order_id, payment_seq NULLS LAST;

CREATE INDEX idx_silver_payments_order ON silver.payments (order_id);
CREATE INDEX idx_silver_payments_type  ON silver.payments (payment_type)
"""

# ── Reviews ───────────────────────────────────────────────────────────────────

SILVER_REVIEWS_SQL = """
DROP TABLE IF EXISTS silver.reviews CASCADE;
CREATE TABLE silver.reviews AS
WITH deduped AS (
    SELECT
        TRIM(review_id)                                             AS review_id,
        TRIM(order_id)                                              AS order_id,
        NULLIF(TRIM(review_score), '')::int                         AS review_score,
        NULLIF(TRIM(review_creation_date),       '')::timestamp     AS review_created_at,
        NULLIF(TRIM(review_answer_timestamp),    '')::timestamp     AS review_answered_at,
        -- Sanitise free-text: trim only (preserve original casing for NLP use)
        NULLIF(TRIM(review_comment_title),   '')                    AS review_title,
        NULLIF(TRIM(review_comment_message), '')                    AS review_message,
        _loaded_at::timestamp                                       AS _loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(review_id)
            ORDER BY _loaded_at DESC
        ) AS _rn
    FROM bronze.reviews
    WHERE NULLIF(TRIM(order_id),     '') IS NOT NULL
      AND NULLIF(TRIM(review_score), '') IS NOT NULL
)
SELECT
    review_id,
    order_id,
    review_score,
    -- Guard: scores must be in the valid 1-5 range
    CASE WHEN review_score BETWEEN 1 AND 5 THEN review_score END    AS review_score_clean,
    review_created_at,
    review_answered_at,
    -- Derived: response lag in hours
    CASE
        WHEN review_answered_at IS NOT NULL AND review_created_at IS NOT NULL
        THEN ROUND(
            EXTRACT(EPOCH FROM (review_answered_at - review_created_at)) / 3600.0,
            1
        )
    END                                                             AS response_lag_hours,
    review_title,
    review_message,
    _loaded_at
FROM deduped
WHERE _rn = 1
ORDER BY review_created_at NULLS LAST, order_id;

CREATE INDEX idx_silver_reviews_order ON silver.reviews (order_id);
CREATE INDEX idx_silver_reviews_score ON silver.reviews (review_score_clean)
"""

# ── Geolocation ───────────────────────────────────────────────────────────────

SILVER_GEOLOCATION_SQL = """
DROP TABLE IF EXISTS silver.geolocation CASCADE;
DROP TYPE IF EXISTS silver.geolocation CASCADE;
CREATE TABLE silver.geolocation AS
WITH validated AS (
    SELECT
        TRIM(geolocation_zip_code_prefix)                           AS zip_code,
        NULLIF(TRIM(geolocation_lat),  '')::numeric(10, 6)          AS latitude,
        NULLIF(TRIM(geolocation_lng),  '')::numeric(10, 6)          AS longitude,
        INITCAP(TRIM(geolocation_city))                             AS city,
        UPPER(TRIM(geolocation_state))                              AS state,
        _loaded_at::timestamp                                       AS _loaded_at
    FROM bronze.geolocation
    WHERE NULLIF(TRIM(geolocation_zip_code_prefix), '') IS NOT NULL
      AND NULLIF(TRIM(geolocation_lat),             '') IS NOT NULL
      AND NULLIF(TRIM(geolocation_lng),             '') IS NOT NULL
),
bounded AS (
    -- Brazil bounding box: lat -33.75 → 5.27, lng -73.99 → -28.85
    SELECT *
    FROM validated
    WHERE latitude  BETWEEN -33.75 AND  5.27
      AND longitude BETWEEN -73.99 AND -28.85
),
-- Keep one representative centroid per zip code (median-ish via avg)
deduped AS (
    SELECT
        zip_code,
        ROUND(AVG(latitude)::numeric,  6)                           AS latitude,
        ROUND(AVG(longitude)::numeric, 6)                           AS longitude,
        -- Most frequent city name per zip (mode approximation via MIN after sorting)
        MIN(city)                                                   AS city,
        MIN(state)                                                  AS state,
        MAX(_loaded_at)                                             AS _loaded_at
    FROM bounded
    GROUP BY zip_code
)
SELECT
    zip_code,
    latitude,
    longitude,
    city,
    state,
    _loaded_at
FROM deduped
ORDER BY state, zip_code;

ALTER TABLE silver.geolocation ADD PRIMARY KEY (zip_code);
CREATE INDEX idx_silver_geolocation_state ON silver.geolocation (state)
"""


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="02_transform_ecommerce_silver",
    description="Clean and type-cast bronze tables into the silver schema",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "transform", "silver"],
)
def transform_ecommerce_silver():

    @task()
    def create_silver_schema():
        """
        Isolated first task — guarantees the silver schema exists before any
        table task runs. Idempotent (IF NOT EXISTS).
        """
        return run_sql("create_silver_schema", CREATE_SILVER_SCHEMA_SQL)

    # ── Silver tables ──────────────────────────────────────────────────────────

    @task()
    def silver_orders():
        return run_sql("silver.orders", SILVER_ORDERS_SQL)

    @task()
    def silver_order_items():
        return run_sql("silver.order_items", SILVER_ORDER_ITEMS_SQL)

    @task()
    def silver_customers():
        return run_sql("silver.customers", SILVER_CUSTOMERS_SQL)

    @task()
    def silver_products():
        return run_sql("silver.products", SILVER_PRODUCTS_SQL)

    @task()
    def silver_sellers():
        return run_sql("silver.sellers", SILVER_SELLERS_SQL)

    @task()
    def silver_payments():
        return run_sql("silver.payments", SILVER_PAYMENTS_SQL)

    @task()
    def silver_reviews():
        return run_sql("silver.reviews", SILVER_REVIEWS_SQL)

    @task()
    def silver_geolocation():
        return run_sql("silver.geolocation", SILVER_GEOLOCATION_SQL)

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def silver_complete(results: list) -> None:
        total_stmts = sum(r.get("statements", 1) for r in results if isinstance(r, dict))
        logger.info("=" * 60)
        logger.info("SILVER TRANSFORM COMPLETE")
        logger.info("=" * 60)
        for r in results:
            if isinstance(r, dict):
                logger.info(f"  ✓ {r['step']:<40} ({r['statements']} statements)")
        logger.info("-" * 60)
        logger.info(f"  Total SQL statements executed: {total_stmts}")

    # ── Dependency graph ───────────────────────────────────────────────────────
    schema = create_silver_schema()

    s_orders      = silver_orders()
    s_items       = silver_order_items()
    s_customers   = silver_customers()
    s_products    = silver_products()
    s_sellers     = silver_sellers()
    s_payments    = silver_payments()
    s_reviews     = silver_reviews()
    s_geolocation = silver_geolocation()

    # All silver tasks wait for the schema to exist
    schema >> [
        s_orders,
        s_items,
        s_customers,
        s_products,
        s_sellers,
        s_payments,
        s_reviews,
        s_geolocation,
    ]

    silver_complete([
        s_orders,
        s_items,
        s_customers,
        s_products,
        s_sellers,
        s_payments,
        s_reviews,
        s_geolocation,
    ])


transform_ecommerce_silver()