"""
dags/03_gold_facts.py
──────────────────────
DAG 3 — gold_ecommerce_facts

Pipeline:
  1. create_gold_schema       — CREATE SCHEMA gold + analytics (idempotent)
  2. build_dim_*              — dimension tables from silver (SCD Type 1)
  3. build_fact_*             — fact tables joining dims + silver
  4. create_bi_views          — public.* views for Power BI / Metabase / Tableau
                                (no schema config needed in BI tools)
  5. gold_complete            — row-count summary log

Silver → Gold transformation rules:
  • Surrogate integer keys     — SERIAL PKs on all dims for BI tool compat
  • Natural-key indexes        — fast join resolution from fact → dim
  • Grain clearly defined      — one row per order / per payment-type per order
  • Additive measures only     — revenue, freight, item counts, fulfilment_days
  • Conformed date dim         — generate_series 2016–2030, ISO fields + flags
  • Referential integrity      — FK-style joins with LEFT JOIN to preserve facts
                                 that arrive before dim rows are populated
  • BI-ready views             — public schema visible by default in all tools

Schedule: daily at 03:00 UTC  (after silver at 02:00)
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
    transaction. Splits on ';' to work around psycopg2's single-statement
    limitation. Commits on success; rolls back entire block on error.
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
        logger.info(f"[{label}] OK — {len(statements)} statement(s)")
    except Exception as exc:
        conn.rollback()
        logger.error(f"[{label}] FAILED — rolled back. Error: {exc}")
        raise
    finally:
        conn.close()
    return {"step": label, "status": "ok", "statements": len(statements)}


def row_count(label: str, table: str) -> dict:
    """Return the row count of a gold table for the summary log."""
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
        logger.info(f"[{label}] {table}: {count:,} rows")
    finally:
        conn.close()
    return {"step": label, "table": table, "rows": count}


# ── Schema creation ───────────────────────────────────────────────────────────

CREATE_SCHEMAS_SQL = """
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS analytics
"""

# ═════════════════════════════════════════════════════════════════════════════
# DIMENSION TABLES
# ─────────────────────────────────────────────────────────────────────────────
# Design:
#   • SERIAL surrogate key (_key) — BI tools prefer integer PKs
#   • Natural business key kept alongside for traceability
#   • SCD Type 1 — full DROP/CREATE (historical changes not required here)
#   • Indexed on both _key and natural key
# ═════════════════════════════════════════════════════════════════════════════

DIM_DATE_SQL = """
DROP TABLE IF EXISTS gold.dim_date CASCADE;
CREATE TABLE gold.dim_date (
    date_key            DATE        PRIMARY KEY,
    year                SMALLINT    NOT NULL,
    quarter             SMALLINT    NOT NULL,
    quarter_label       CHAR(6)     NOT NULL,   -- e.g. 2018Q1
    month               SMALLINT    NOT NULL,
    month_name          VARCHAR(9)  NOT NULL,
    month_short         CHAR(3)     NOT NULL,   -- Jan, Feb …
    week_of_year        SMALLINT    NOT NULL,
    day_of_month        SMALLINT    NOT NULL,
    day_of_week         SMALLINT    NOT NULL,   -- 0 = Sun … 6 = Sat
    day_name            VARCHAR(9)  NOT NULL,
    day_short           CHAR(3)     NOT NULL,   -- Mon, Tue …
    is_weekend          BOOLEAN     NOT NULL,
    is_month_start      BOOLEAN     NOT NULL,
    is_month_end        BOOLEAN     NOT NULL,
    days_in_month       SMALLINT    NOT NULL,
    day_of_year         SMALLINT    NOT NULL,
    fiscal_year         SMALLINT    NOT NULL,   -- Jan fiscal year start
    fiscal_quarter      SMALLINT    NOT NULL
);
INSERT INTO gold.dim_date
SELECT
    d::date                                             AS date_key,
    EXTRACT(YEAR    FROM d)::smallint                   AS year,
    EXTRACT(QUARTER FROM d)::smallint                   AS quarter,
    TO_CHAR(d, 'YYYY"Q"Q')                              AS quarter_label,
    EXTRACT(MONTH   FROM d)::smallint                   AS month,
    TO_CHAR(d, 'FMMonth')                               AS month_name,
    TO_CHAR(d, 'Mon')                                   AS month_short,
    EXTRACT(WEEK    FROM d)::smallint                   AS week_of_year,
    EXTRACT(DAY     FROM d)::smallint                   AS day_of_month,
    EXTRACT(DOW     FROM d)::smallint                   AS day_of_week,
    TO_CHAR(d, 'FMDay')                                 AS day_name,
    TO_CHAR(d, 'Dy')                                    AS day_short,
    EXTRACT(DOW FROM d) IN (0, 6)                       AS is_weekend,
    (d = DATE_TRUNC('month', d))                        AS is_month_start,
    (d = (DATE_TRUNC('month', d) + INTERVAL '1 month - 1 day')::date) AS is_month_end,
    EXTRACT(DAYS FROM (
        DATE_TRUNC('month', d) + INTERVAL '1 month'
      - DATE_TRUNC('month', d)
    ))::smallint                                        AS days_in_month,
    EXTRACT(DOY FROM d)::smallint                       AS day_of_year,
    EXTRACT(YEAR FROM d)::smallint                      AS fiscal_year,
    EXTRACT(QUARTER FROM d)::smallint                   AS fiscal_quarter
FROM generate_series(
    '2016-01-01'::date,
    '2030-12-31'::date,
    '1 day'::interval
) AS gs(d);
CREATE INDEX idx_gold_dim_date_year    ON gold.dim_date (year);
CREATE INDEX idx_gold_dim_date_month   ON gold.dim_date (year, month);
CREATE INDEX idx_gold_dim_date_quarter ON gold.dim_date (year, quarter)
"""

DIM_CUSTOMER_SQL = """
DROP TABLE IF EXISTS gold.dim_customer CASCADE;

CREATE TABLE gold.dim_customer AS
WITH unique_customers AS (
    -- Step 1: Deduplicate customers first
    SELECT DISTINCT ON (customer_unique_id)
        customer_unique_id,
        city,
        state,
        zip_code,
        _loaded_at
    FROM silver.customers
    ORDER BY customer_unique_id, _loaded_at DESC
),
unique_geo AS (
    -- Step 2: Deduplicate geolocation 
    SELECT DISTINCT ON (zip_code)
        zip_code,
        latitude,
        longitude
    FROM silver.geolocation
    ORDER BY zip_code
)
SELECT
    c.customer_unique_id                 AS customer_key,
    -- Since we've already selected 1 row per unique_id, we can just grab these
    c.city,
    c.state,
    c.zip_code,
    g.latitude,
    g.longitude,
    c._loaded_at
FROM unique_customers c
LEFT JOIN unique_geo g ON c.zip_code = g.zip_code;

-- This will now succeed because unique_customers only has 1 row per ID
ALTER TABLE gold.dim_customer ADD PRIMARY KEY (customer_key);
CREATE INDEX idx_gold_dim_customer_state ON gold.dim_customer (state);
"""

DIM_PRODUCT_SQL = """
DROP TABLE IF EXISTS gold.dim_product CASCADE;
CREATE TABLE gold.dim_product AS
SELECT
    product_id          AS product_key,
    category,
    weight_g,
    length_cm,
    height_cm,
    width_cm,
    volumetric_weight_kg,
    photos_qty,
    description_len,
    name_len,
    -- Derived: size bucket for BI grouping
    CASE
        WHEN volumetric_weight_kg IS NULL   THEN 'unknown'
        WHEN volumetric_weight_kg <  0.5    THEN 'small'
        WHEN volumetric_weight_kg <  5.0    THEN 'medium'
        WHEN volumetric_weight_kg < 20.0    THEN 'large'
        ELSE 'extra-large'
    END                 AS size_bucket,
    _loaded_at
FROM silver.products;
ALTER TABLE gold.dim_product ADD PRIMARY KEY (product_key);
CREATE INDEX idx_gold_dim_product_category ON gold.dim_product (category);
CREATE INDEX idx_gold_dim_product_size     ON gold.dim_product (size_bucket)
"""

DIM_SELLER_SQL = """
DROP TABLE IF EXISTS gold.dim_seller CASCADE;
CREATE TABLE gold.dim_seller AS
SELECT
    s.seller_id         AS seller_key,
    s.zip_code,
    s.city,
    s.state,
    g.latitude,
    g.longitude,
    s._loaded_at
FROM silver.sellers s
LEFT JOIN silver.geolocation g USING (zip_code);
ALTER TABLE gold.dim_seller ADD PRIMARY KEY (seller_key);
CREATE INDEX idx_gold_dim_seller_state ON gold.dim_seller (state)
"""

DIM_PAYMENT_TYPE_SQL = """
DROP TABLE IF EXISTS gold.dim_payment_type CASCADE;
CREATE TABLE gold.dim_payment_type AS
SELECT
    ROW_NUMBER() OVER (ORDER BY payment_type)::int  AS payment_type_key,
    payment_type,
    -- Human-readable label for BI
    CASE payment_type
        WHEN 'credit_card'  THEN 'Credit Card'
        WHEN 'boleto'       THEN 'Boleto'
        WHEN 'voucher'      THEN 'Voucher'
        WHEN 'debit_card'   THEN 'Debit Card'
        ELSE INITCAP(REPLACE(payment_type, '_', ' '))
    END                                             AS payment_label,
    -- Grouping flag for high-level reporting
    CASE payment_type
        WHEN 'credit_card' THEN 'card'
        WHEN 'debit_card'  THEN 'card'
        ELSE 'other'
    END                                             AS payment_group
FROM (
    SELECT DISTINCT payment_type FROM silver.payments
) t;
ALTER TABLE gold.dim_payment_type ADD PRIMARY KEY (payment_type_key);
CREATE UNIQUE INDEX idx_gold_dim_ptype_name ON gold.dim_payment_type (payment_type)
"""

# ═════════════════════════════════════════════════════════════════════════════
# FACT TABLES
# ─────────────────────────────────────────────────────────────────────────────
# Design:
#   • Grain clearly stated in comments
#   • All FK references to gold.dim_* via natural keys (no integer surrogate
#     joins needed — date_key is the date itself, customer_key is the UUID)
#   • Only additive / semi-additive measures stored
#   • Non-additive ratios (avg score, on-time %) computed in BI layer
#   • Indexed on every FK + date for fast slicing
# ═════════════════════════════════════════════════════════════════════════════

FACT_ORDERS_SQL = """
DROP TABLE IF EXISTS gold.fact_orders CASCADE;
CREATE TABLE gold.fact_orders AS
-- GRAIN: one row per order
SELECT
    o.order_id,

    -- Foreign keys
    c.customer_unique_id                            AS customer_key,
    o.purchased_at::date                            AS order_date_key,
    o.shipped_at::date                              AS ship_date_key,
    o.delivered_at::date                            AS delivery_date_key,
    o.estimated_delivery_at::date                   AS est_delivery_date_key,

    -- Order status
    o.order_status,

    -- Delivery metrics
    o.fulfilment_days,
    o.delivered_on_time,
    CASE
        WHEN o.delivered_on_time IS NULL THEN 'pending'
        WHEN o.delivered_on_time         THEN 'on_time'
        ELSE 'late'
    END                                             AS delivery_status,

    -- Item measures
    COUNT(DISTINCT i.product_id)::int               AS distinct_products,
    COUNT(DISTINCT i.seller_id)::int                AS distinct_sellers,
    SUM(i.item_seq)::int                            AS total_items,

    -- Revenue measures (all additive)
    COALESCE(SUM(i.unit_price),    0)::numeric(14,2) AS product_revenue,
    COALESCE(SUM(i.freight_value), 0)::numeric(14,2) AS freight_revenue,
    COALESCE(SUM(i.line_total),    0)::numeric(14,2) AS gross_revenue,

    -- Payment measures (aggregated from silver.payments)
    COALESCE(pay.total_paid,    0)::numeric(14,2)   AS total_paid,
    pay.payment_types_used::int,

    -- Review measure
    r.review_score_clean                            AS review_score,
    r.response_lag_hours,

    -- Audit
    o._loaded_at
FROM silver.orders o

-- Customer dim lookup
JOIN silver.customers c
    ON o.customer_id = c.customer_id

-- Line items (LEFT — an order may have no items yet)
LEFT JOIN silver.order_items i
    ON o.order_id = i.order_id

-- Payment rollup per order
LEFT JOIN (
    SELECT
        order_id,
        SUM(amount)                             AS total_paid,
        COUNT(DISTINCT payment_type)::int       AS payment_types_used
    FROM silver.payments
    GROUP BY order_id
) pay ON o.order_id = pay.order_id

-- Latest review per order
LEFT JOIN (
    SELECT DISTINCT ON (order_id)
        order_id,
        review_score_clean,
        response_lag_hours
    FROM silver.reviews
    ORDER BY order_id, review_created_at DESC NULLS LAST
) r ON o.order_id = r.order_id

GROUP BY
    o.order_id,
    c.customer_unique_id,
    o.purchased_at, o.shipped_at, o.delivered_at, o.estimated_delivery_at,
    o.order_status, o.fulfilment_days, o.delivered_on_time,
    pay.total_paid, pay.payment_types_used,
    r.review_score_clean, r.response_lag_hours,
    o._loaded_at;

ALTER TABLE gold.fact_orders ADD PRIMARY KEY (order_id);
CREATE INDEX idx_gold_fact_orders_customer  ON gold.fact_orders (customer_key);
CREATE INDEX idx_gold_fact_orders_date      ON gold.fact_orders (order_date_key);
CREATE INDEX idx_gold_fact_orders_ship      ON gold.fact_orders (ship_date_key);
CREATE INDEX idx_gold_fact_orders_status    ON gold.fact_orders (order_status);
CREATE INDEX idx_gold_fact_orders_score     ON gold.fact_orders (review_score);
CREATE INDEX idx_gold_fact_orders_delivered ON gold.fact_orders (delivery_status)
"""

FACT_ORDER_ITEMS_SQL = """
DROP TABLE IF EXISTS gold.fact_order_items CASCADE;
CREATE TABLE gold.fact_order_items AS
-- GRAIN: one row per order × product × seller (finest level of detail)
SELECT
    i.order_id,
    i.item_seq,

    -- Foreign keys
    o.purchased_at::date                            AS order_date_key,
    c.customer_unique_id                            AS customer_key,
    i.product_id                                    AS product_key,
    i.seller_id                                     AS seller_key,

    -- Order context
    o.order_status,
    o.delivered_on_time,

    -- Line-level measures
    i.unit_price,
    i.freight_value,
    i.line_total,

    -- Shipping deadline adherence at item level
    CASE
        WHEN i.shipping_limit_at IS NOT NULL AND o.shipped_at IS NOT NULL
        THEN (o.shipped_at <= i.shipping_limit_at)
    END                                             AS shipped_before_limit,

    i._loaded_at
FROM silver.order_items i
JOIN silver.orders   o ON i.order_id   = o.order_id
JOIN silver.customers c ON o.customer_id = c.customer_id;

CREATE INDEX idx_gold_fact_items_order    ON gold.fact_order_items (order_id);
CREATE INDEX idx_gold_fact_items_date     ON gold.fact_order_items (order_date_key);
CREATE INDEX idx_gold_fact_items_product  ON gold.fact_order_items (product_key);
CREATE INDEX idx_gold_fact_items_seller   ON gold.fact_order_items (seller_key);
CREATE INDEX idx_gold_fact_items_customer ON gold.fact_order_items (customer_key)
"""

FACT_PAYMENTS_SQL = """
DROP TABLE IF EXISTS gold.fact_payments CASCADE;
CREATE TABLE gold.fact_payments AS
-- GRAIN: one row per order × payment_type (collapsed installments)
SELECT
    p.order_id,

    -- Foreign keys
    o.purchased_at::date                            AS order_date_key,
    c.customer_unique_id                            AS customer_key,
    pt.payment_type_key,
    p.payment_type,

    -- Payment measures
    SUM(p.amount)::numeric(14,2)                    AS total_amount,
    COUNT(*)::int                                   AS payment_rows,
    MAX(p.installments)::int                        AS max_installments,
    MIN(p.installments)::int                        AS min_installments,

    -- Instalment flag for BI filtering
    (MAX(p.installments) > 1)                       AS is_instalment,

    o._loaded_at
FROM silver.payments p
JOIN silver.orders   o  ON p.order_id   = o.order_id
JOIN silver.customers c  ON o.customer_id = c.customer_id
JOIN gold.dim_payment_type pt ON p.payment_type = pt.payment_type
GROUP BY
    p.order_id,
    o.purchased_at,
    c.customer_unique_id,
    pt.payment_type_key,
    p.payment_type,
    o._loaded_at;

CREATE INDEX idx_gold_fact_pay_order    ON gold.fact_payments (order_id);
CREATE INDEX idx_gold_fact_pay_date     ON gold.fact_payments (order_date_key);
CREATE INDEX idx_gold_fact_pay_type     ON gold.fact_payments (payment_type_key);
CREATE INDEX idx_gold_fact_pay_customer ON gold.fact_payments (customer_key)
"""

FACT_REVIEWS_SQL = """
DROP TABLE IF EXISTS gold.fact_reviews CASCADE;
CREATE TABLE gold.fact_reviews AS
-- GRAIN: one row per review (a review is tied to one order)
SELECT
    r.review_id,
    r.order_id,

    -- Foreign keys
    o.purchased_at::date                            AS order_date_key,
    r.review_created_at::date                       AS review_date_key,
    c.customer_unique_id                            AS customer_key,

    -- Review measures
    r.review_score_clean                            AS review_score,
    r.response_lag_hours,

    -- Sentiment bucket for easy BI filtering
    CASE
        WHEN r.review_score_clean >= 4 THEN 'positive'
        WHEN r.review_score_clean  = 3 THEN 'neutral'
        WHEN r.review_score_clean <= 2 THEN 'negative'
        ELSE 'unscored'
    END                                             AS sentiment,

    -- Text flags (useful for filtering in NLP pipelines)
    (r.review_title   IS NOT NULL)                  AS has_title,
    (r.review_message IS NOT NULL)                  AS has_message,

    r._loaded_at
FROM silver.reviews r
JOIN silver.orders    o ON r.order_id    = o.order_id
JOIN silver.customers c ON o.customer_id = c.customer_id
WHERE r.review_id IS NOT NULL;

ALTER TABLE gold.fact_reviews ADD PRIMARY KEY (review_id);
CREATE INDEX idx_gold_fact_reviews_order     ON gold.fact_reviews (order_id);
CREATE INDEX idx_gold_fact_reviews_date      ON gold.fact_reviews (review_date_key);
CREATE INDEX idx_gold_fact_reviews_customer  ON gold.fact_reviews (customer_key);
CREATE INDEX idx_gold_fact_reviews_score     ON gold.fact_reviews (review_score);
CREATE INDEX idx_gold_fact_reviews_sentiment ON gold.fact_reviews (sentiment)
"""

# ═════════════════════════════════════════════════════════════════════════════
# ANALYTICS SCHEMA — AGGREGATED SUMMARY TABLES
# ─────────────────────────────────────────────────────────────────────────────
# Pre-aggregated tables for dashboards. Querying these is orders of magnitude
# faster than hitting raw fact tables for every chart render.
# ═════════════════════════════════════════════════════════════════════════════

AGG_MONTHLY_REVENUE_SQL = """
DROP TABLE IF EXISTS analytics.monthly_revenue CASCADE;
CREATE TABLE analytics.monthly_revenue AS
SELECT
    d.year,
    d.month,
    d.month_short,
    d.quarter,
    d.quarter_label,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    COUNT(DISTINCT f.customer_key)      AS unique_customers,
    SUM(f.gross_revenue)::numeric(16,2) AS gross_revenue,
    SUM(f.product_revenue)::numeric(16,2) AS product_revenue,
    SUM(f.freight_revenue)::numeric(16,2) AS freight_revenue,
    SUM(f.total_items)                  AS total_items,
    ROUND(AVG(f.gross_revenue)::numeric, 2) AS avg_order_value,
    ROUND(AVG(f.review_score)::numeric, 2)  AS avg_review_score,
    SUM(CASE WHEN f.delivered_on_time THEN 1 ELSE 0 END) AS on_time_deliveries,
    COUNT(CASE WHEN f.delivered_on_time IS NOT NULL THEN 1 END) AS deliveries_with_status,
    ROUND(
        100.0 * SUM(CASE WHEN f.delivered_on_time THEN 1 ELSE 0 END)
        / NULLIF(COUNT(CASE WHEN f.delivered_on_time IS NOT NULL THEN 1 END), 0),
        1
    )                                   AS on_time_pct
FROM gold.fact_orders f
JOIN gold.dim_date d ON f.order_date_key = d.date_key
WHERE f.order_status NOT IN ('canceled', 'unavailable')
GROUP BY d.year, d.month, d.month_short, d.quarter, d.quarter_label
ORDER BY d.year, d.month;
CREATE UNIQUE INDEX idx_agg_monthly_rev ON analytics.monthly_revenue (year, month)
"""

AGG_CATEGORY_PERFORMANCE_SQL = """
DROP TABLE IF EXISTS analytics.category_performance CASCADE;
CREATE TABLE analytics.category_performance AS
SELECT
    p.category,
    p.size_bucket,
    COUNT(DISTINCT fi.order_id)             AS total_orders,
    COUNT(DISTINCT fi.customer_key)         AS unique_customers,
    COUNT(DISTINCT fi.seller_key)           AS unique_sellers,
    SUM(fi.line_total)::numeric(16,2)       AS gross_revenue,
    SUM(fi.unit_price)::numeric(16,2)       AS product_revenue,
    SUM(fi.freight_value)::numeric(16,2)    AS freight_revenue,
    COUNT(*)                                AS total_line_items,
    ROUND(AVG(fi.unit_price)::numeric, 2)   AS avg_unit_price,
    ROUND(AVG(fi.freight_value)::numeric,2) AS avg_freight,
    ROUND(
        100.0 * SUM(fi.freight_value)
        / NULLIF(SUM(fi.line_total), 0),
        1
    )                                       AS freight_pct
FROM gold.fact_order_items fi
JOIN gold.dim_product p ON fi.product_key = p.product_key
WHERE fi.order_status NOT IN ('canceled', 'unavailable')
GROUP BY p.category, p.size_bucket
ORDER BY gross_revenue DESC;
CREATE UNIQUE INDEX idx_agg_cat_perf ON analytics.category_performance (category, size_bucket)
"""

AGG_SELLER_PERFORMANCE_SQL = """
DROP TABLE IF EXISTS analytics.seller_performance CASCADE;
CREATE TABLE analytics.seller_performance AS
SELECT
    s.seller_key,
    s.city                                  AS seller_city,
    s.state                                 AS seller_state,
    COUNT(DISTINCT fi.order_id)             AS total_orders,
    COUNT(*)                                AS total_line_items,
    COUNT(DISTINCT fi.product_key)          AS distinct_products,
    SUM(fi.line_total)::numeric(16,2)       AS gross_revenue,
    ROUND(AVG(fi.unit_price)::numeric, 2)   AS avg_unit_price,
    ROUND(AVG(fo.review_score)::numeric, 2) AS avg_review_score,
    ROUND(
        100.0 * SUM(CASE WHEN fi.shipped_before_limit THEN 1 ELSE 0 END)
        / NULLIF(COUNT(CASE WHEN fi.shipped_before_limit IS NOT NULL THEN 1 END), 0),
        1
    )                                       AS ship_on_time_pct
FROM gold.fact_order_items fi
JOIN gold.dim_seller  s  ON fi.seller_key  = s.seller_key
JOIN gold.fact_orders fo ON fi.order_id    = fo.order_id
WHERE fi.order_status NOT IN ('canceled', 'unavailable')
GROUP BY s.seller_key, s.city, s.state
ORDER BY gross_revenue DESC;
CREATE UNIQUE INDEX idx_agg_seller_perf ON analytics.seller_performance (seller_key)
"""

AGG_CUSTOMER_SEGMENTS_SQL = """
DROP TABLE IF EXISTS analytics.customer_segments CASCADE;
CREATE TABLE analytics.customer_segments AS
-- RFM-style segmentation (Recency / Frequency / Monetary)
WITH rfm AS (
    SELECT
        customer_key,
        MAX(order_date_key)                         AS last_order_date,
        COUNT(DISTINCT order_id)                    AS order_count,
        SUM(gross_revenue)::numeric(14,2)           AS lifetime_value,
        ROUND(AVG(gross_revenue)::numeric, 2)       AS avg_order_value,
        ROUND(AVG(review_score)::numeric, 2)        AS avg_review_score,
        (CURRENT_DATE - MAX(order_date_key))::int   AS recency_days
    FROM gold.fact_orders
    WHERE order_status NOT IN ('canceled', 'unavailable')
    GROUP BY customer_key
),
scored AS (
    SELECT *,
        NTILE(4) OVER (ORDER BY recency_days   ASC)  AS r_score,  -- 4 = most recent
        NTILE(4) OVER (ORDER BY order_count    ASC)  AS f_score,
        NTILE(4) OVER (ORDER BY lifetime_value ASC)  AS m_score
    FROM rfm
)
SELECT
    s.customer_key,
    dc.city,
    dc.state,
    s.last_order_date,
    s.recency_days,
    s.order_count,
    s.lifetime_value,
    s.avg_order_value,
    s.avg_review_score,
    s.r_score,
    s.f_score,
    s.m_score,
    (s.r_score + s.f_score + s.m_score)             AS rfm_total,
    -- Segment label for BI legend
    CASE
        WHEN s.r_score = 4 AND s.f_score >= 3 AND s.m_score >= 3 THEN 'Champions'
        WHEN s.r_score >= 3 AND s.f_score >= 3                    THEN 'Loyal'
        WHEN s.r_score = 4 AND s.f_score <= 2                     THEN 'Recent'
        WHEN s.r_score >= 3 AND s.m_score >= 3                    THEN 'Big Spenders'
        WHEN s.r_score <= 2 AND s.f_score >= 3                    THEN 'At Risk'
        WHEN s.r_score = 1 AND s.f_score = 1                      THEN 'Lost'
        ELSE 'Occasional'
    END                                             AS segment
FROM scored s
JOIN gold.dim_customer dc ON s.customer_key = dc.customer_key
ORDER BY lifetime_value DESC;
CREATE UNIQUE INDEX idx_agg_cust_seg      ON analytics.customer_segments (customer_key);
CREATE INDEX        idx_agg_cust_seg_seg  ON analytics.customer_segments (segment)
"""

# ═════════════════════════════════════════════════════════════════════════════
# BI VIEWS — public schema (visible by default in all BI tools)
# ═════════════════════════════════════════════════════════════════════════════

BI_VIEWS_SQL = """
CREATE OR REPLACE VIEW public.dim_date               AS SELECT * FROM gold.dim_date;
CREATE OR REPLACE VIEW public.dim_customer           AS SELECT * FROM gold.dim_customer;
CREATE OR REPLACE VIEW public.dim_product            AS SELECT * FROM gold.dim_product;
CREATE OR REPLACE VIEW public.dim_seller             AS SELECT * FROM gold.dim_seller;
CREATE OR REPLACE VIEW public.dim_payment_type       AS SELECT * FROM gold.dim_payment_type;
CREATE OR REPLACE VIEW public.fact_orders            AS SELECT * FROM gold.fact_orders;
CREATE OR REPLACE VIEW public.fact_order_items       AS SELECT * FROM gold.fact_order_items;
CREATE OR REPLACE VIEW public.fact_payments          AS SELECT * FROM gold.fact_payments;
CREATE OR REPLACE VIEW public.fact_reviews           AS SELECT * FROM gold.fact_reviews;
CREATE OR REPLACE VIEW public.monthly_revenue        AS SELECT * FROM analytics.monthly_revenue;
CREATE OR REPLACE VIEW public.category_performance   AS SELECT * FROM analytics.category_performance;
CREATE OR REPLACE VIEW public.seller_performance     AS SELECT * FROM analytics.seller_performance;
CREATE OR REPLACE VIEW public.customer_segments      AS SELECT * FROM analytics.customer_segments
"""


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="03_gold_ecommerce_facts",
    description="Build gold dims, facts, aggregates and BI views from silver",
    schedule="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "gold", "facts", "analytics"],
)
def gold_ecommerce_facts():

    # ── Schema ─────────────────────────────────────────────────────────────────
    @task()
    def create_schemas():
        return run_sql("create_schemas", CREATE_SCHEMAS_SQL)

    # ── Dimensions ─────────────────────────────────────────────────────────────
    @task()
    def build_dim_date():
        return run_sql("dim_date", DIM_DATE_SQL)

    @task()
    def build_dim_customer():
        return run_sql("dim_customer", DIM_CUSTOMER_SQL)

    @task()
    def build_dim_product():
        return run_sql("dim_product", DIM_PRODUCT_SQL)

    @task()
    def build_dim_seller():
        return run_sql("dim_seller", DIM_SELLER_SQL)

    @task()
    def build_dim_payment_type():
        return run_sql("dim_payment_type", DIM_PAYMENT_TYPE_SQL)

    # ── Facts ──────────────────────────────────────────────────────────────────
    @task()
    def build_fact_orders():
        return run_sql("fact_orders", FACT_ORDERS_SQL)

    @task()
    def build_fact_order_items():
        return run_sql("fact_order_items", FACT_ORDER_ITEMS_SQL)

    @task()
    def build_fact_payments():
        return run_sql("fact_payments", FACT_PAYMENTS_SQL)

    @task()
    def build_fact_reviews():
        return run_sql("fact_reviews", FACT_REVIEWS_SQL)

    # ── Aggregates ─────────────────────────────────────────────────────────────
    @task()
    def build_agg_monthly_revenue():
        return run_sql("agg_monthly_revenue", AGG_MONTHLY_REVENUE_SQL)

    @task()
    def build_agg_category_performance():
        return run_sql("agg_category_performance", AGG_CATEGORY_PERFORMANCE_SQL)

    @task()
    def build_agg_seller_performance():
        return run_sql("agg_seller_performance", AGG_SELLER_PERFORMANCE_SQL)

    @task()
    def build_agg_customer_segments():
        return run_sql("agg_customer_segments", AGG_CUSTOMER_SEGMENTS_SQL)

    # ── BI Views ───────────────────────────────────────────────────────────────
    @task()
    def create_bi_views():
        return run_sql("bi_views", BI_VIEWS_SQL)

    # ── Summary ────────────────────────────────────────────────────────────────
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def gold_complete(results: list) -> None:
        total_stmts = sum(r.get("statements", 1) for r in results if isinstance(r, dict))
        logger.info("=" * 65)
        logger.info("GOLD LAYER COMPLETE")
        logger.info("=" * 65)
        for r in results:
            if isinstance(r, dict):
                logger.info(f"  ✓ {r['step']:<45} ({r['statements']} stmts)")
        logger.info("-" * 65)
        logger.info(f"  Total SQL statements executed: {total_stmts}")

    # ── Dependency graph ───────────────────────────────────────────────────────
    schemas = create_schemas()

    # All dims wait for schema
    dim_date    = build_dim_date()
    dim_cust    = build_dim_customer()
    dim_prod    = build_dim_product()
    dim_sell    = build_dim_seller()
    dim_ptype   = build_dim_payment_type()

    schemas >> [dim_date, dim_cust, dim_prod, dim_sell, dim_ptype]

    # Facts wait for their required dims
    fact_ord    = build_fact_orders()
    fact_items  = build_fact_order_items()
    fact_pay    = build_fact_payments()
    fact_rev    = build_fact_reviews()

    [dim_date, dim_cust, dim_prod, dim_sell]  >> fact_ord
    [dim_date, dim_cust, dim_prod, dim_sell]  >> fact_items
    [dim_date, dim_cust, dim_ptype]           >> fact_pay
    [dim_date, dim_cust]                      >> fact_rev

    # Aggregates wait for facts
    agg_rev   = build_agg_monthly_revenue()
    agg_cat   = build_agg_category_performance()
    agg_sell  = build_agg_seller_performance()
    agg_cust  = build_agg_customer_segments()

    [fact_ord, fact_items]              >> agg_rev
    [fact_ord, fact_items, dim_prod]    >> agg_cat
    [fact_ord, fact_items, dim_sell]    >> agg_sell
    [fact_ord, dim_cust]                >> agg_cust

    # BI views wait for everything
    views = create_bi_views()
    [
        fact_ord, fact_items, fact_pay, fact_rev,
        dim_date, dim_cust, dim_prod, dim_sell, dim_ptype,
        agg_rev, agg_cat, agg_sell, agg_cust,
    ] >> views

    gold_complete([
        dim_date, dim_cust, dim_prod, dim_sell, dim_ptype,
        fact_ord, fact_items, fact_pay, fact_rev,
        agg_rev, agg_cat, agg_sell, agg_cust,
        views,
    ])


gold_ecommerce_facts()