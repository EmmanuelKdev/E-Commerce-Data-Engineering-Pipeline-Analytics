"""
dags/04_quality_checks.py
──────────────────────────
DAG 4 — quality_checks_ecommerce

Validates the full medallion stack (bronze → silver → gold → analytics)
after all transform DAGs complete.

Layer coverage:
  • Bronze  — source file row counts, no completely empty tables
  • Silver  — null rates on mandatory keys, type-cast sanity, dedup checks
  • Gold    — dim/fact row minimums, referential integrity, grain uniqueness
  • Analytics — aggregate sanity, business rule assertions, RFM completeness

Schedule: daily at 04:00 UTC  (after gold DAG at 03:00)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

CONN_ID = "postgres_warehouse"

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}

# ── Row count minimums ────────────────────────────────────────────────────────
# Bronze: raw CSVs loaded verbatim — thresholds are intentionally permissive.
# Silver: post-dedup / post-filter counts will be slightly lower than bronze.
# Gold:   facts and dims built from silver — same order of magnitude as silver.
# Analytics: aggregates will be much smaller (grouped).

ROW_COUNT_MINIMUMS = {
    # Bronze
    "bronze.orders":                       90_000,
    "bronze.order_items":                 110_000,
    "bronze.customers":                    90_000,
    "bronze.products":                     30_000,
    "bronze.sellers":                       3_000,
    "bronze.payments":                    100_000,
    "bronze.reviews":                      90_000,
    "bronze.geolocation":                 100_000,
    # Silver
    "silver.orders":                       89_000,
    "silver.order_items":                 108_000,
    "silver.customers":                    88_000,
    "silver.products":                     29_000,
    "silver.sellers":                       2_900,
    "silver.payments":                     99_000,
    "silver.reviews":                      88_000,
    "silver.geolocation":                  10_000,   # deduplicated to unique zip codes
    # Gold dims
    "gold.dim_date":                        5_000,   # 2016–2030 = ~5 480 days
    "gold.dim_customer":                   90_000,
    "gold.dim_product":                    29_000,
    "gold.dim_seller":                      2_900,
    "gold.dim_payment_type":                    4,
    # Gold facts
    "gold.fact_orders":                    89_000,
    "gold.fact_order_items":              108_000,
    "gold.fact_payments":                  99_000,
    "gold.fact_reviews":                   88_000,
    # Analytics aggregates
    "analytics.monthly_revenue":               36,   # ~3 years of data → ≥ 36 months
    "analytics.category_performance":          50,
    "analytics.seller_performance":          2_900,
    "analytics.customer_segments":          90_000,
}

# ── Null-rate checks (table, column, max_allowed_null_fraction) ───────────────
# Format: (schema.table, column, max_null_rate_as_decimal)

NULL_RATE_CHECKS = [
    # ── Silver mandatory keys ─────────────────────────────────────────────────
    ("silver.orders",       "order_id",             0.00),
    ("silver.orders",       "customer_id",          0.00),
    ("silver.orders",       "order_status",         0.00),
    ("silver.orders",       "purchased_at",         0.01),   # tiny tolerance for edge rows
    ("silver.order_items",  "order_id",             0.00),
    ("silver.order_items",  "product_id",           0.00),
    ("silver.order_items",  "seller_id",            0.00),
    ("silver.order_items",  "unit_price",           0.00),
    ("silver.customers",    "customer_id",          0.00),
    ("silver.customers",    "customer_unique_id",   0.00),
    ("silver.products",     "product_id",           0.00),
    ("silver.products",     "category",             0.00),   # COALESCE → 'unknown', never NULL
    ("silver.sellers",      "seller_id",            0.00),
    ("silver.payments",     "order_id",             0.00),
    ("silver.payments",     "amount",               0.00),
    ("silver.reviews",      "order_id",             0.00),
    ("silver.reviews",      "review_score_clean",   0.10),   # some reviews lack a valid score
    # ── Gold dim keys ─────────────────────────────────────────────────────────
    ("gold.dim_customer",    "customer_key",        0.00),
    ("gold.dim_product",     "product_key",         0.00),
    ("gold.dim_seller",      "seller_key",          0.00),
    ("gold.dim_payment_type","payment_type",        0.00),
    # ── Gold fact keys ────────────────────────────────────────────────────────
    ("gold.fact_orders",     "order_id",            0.00),
    ("gold.fact_orders",     "customer_key",        0.00),
    ("gold.fact_orders",     "order_date_key",      0.01),
    ("gold.fact_orders",     "gross_revenue",       0.00),
    ("gold.fact_orders",     "order_status",        0.00),
    ("gold.fact_order_items","order_id",            0.00),
    ("gold.fact_order_items","product_key",         0.00),
    ("gold.fact_order_items","seller_key",          0.00),
    ("gold.fact_order_items","unit_price",          0.00),
    ("gold.fact_payments",   "order_id",            0.00),
    ("gold.fact_payments",   "total_amount",        0.00),
    ("gold.fact_payments",   "payment_type_key",    0.00),
    ("gold.fact_reviews",    "order_id",            0.00),
    ("gold.fact_reviews",    "review_score",        0.10),
]

# ── Business rule checks ──────────────────────────────────────────────────────

BUSINESS_RULE_CHECKS = [
    # ── Silver sanity ─────────────────────────────────────────────────────────
    {
        "name": "silver_no_negative_prices",
        "sql": "SELECT COUNT(*) FROM silver.order_items WHERE unit_price <= 0",
        "expected": 0,
        "description": "silver.order_items: unit_price must be > 0 (filtered in transform)",
    },
    {
        "name": "silver_no_zero_payments",
        "sql": "SELECT COUNT(*) FROM silver.payments WHERE amount <= 0",
        "expected": 0,
        "description": "silver.payments: amount must be > 0 (filtered in transform)",
    },
    {
        "name": "silver_valid_review_scores",
        "sql": """
            SELECT COUNT(*) FROM silver.reviews
            WHERE review_score_clean IS NOT NULL
              AND review_score_clean NOT BETWEEN 1 AND 5
        """,
        "expected": 0,
        "description": "silver.reviews: review_score_clean must be 1–5 or NULL",
    },
    {
        "name": "silver_geolocation_in_brazil",
        "sql": """
            SELECT COUNT(*) FROM silver.geolocation
            WHERE latitude  NOT BETWEEN -33.75 AND  5.27
               OR longitude NOT BETWEEN -73.99 AND -28.85
        """,
        "expected": 0,
        "description": "silver.geolocation: all coordinates must fall within Brazil's bounding box",
    },
    {
        "name": "silver_orders_no_duplicate_order_ids",
        "sql": """
            SELECT COUNT(*) FROM (
                SELECT order_id FROM silver.orders
                GROUP BY order_id HAVING COUNT(*) > 1
            ) dups
        """,
        "expected": 0,
        "description": "silver.orders: order_id must be unique after deduplication",
    },
    {
        "name": "silver_customers_no_duplicate_customer_ids",
        "sql": """
            SELECT COUNT(*) FROM (
                SELECT customer_id FROM silver.customers
                GROUP BY customer_id HAVING COUNT(*) > 1
            ) dups
        """,
        "expected": 0,
        "description": "silver.customers: customer_id must be unique after deduplication",
    },
    {
        "name": "silver_payments_default_installments",
        "sql": "SELECT COUNT(*) FROM silver.payments WHERE installments < 1",
        "expected": 0,
        "description": "silver.payments: installments must be ≥ 1 (COALESCE default applied)",
    },
    # ── Gold dim integrity ────────────────────────────────────────────────────
    {
        "name": "gold_dim_date_no_gaps",
        "sql": """
            SELECT COUNT(*) FROM (
                SELECT date_key,
                       LEAD(date_key) OVER (ORDER BY date_key) AS next_key
                FROM gold.dim_date
            ) t
            WHERE next_key - date_key > 1
        """,
        "expected": 0,
        "description": "gold.dim_date: no gaps in the daily calendar sequence",
    },
    {
        "name": "gold_dim_date_covers_fact_orders",
        "sql": """
            SELECT COUNT(*) FROM gold.fact_orders f
            LEFT JOIN gold.dim_date d ON f.order_date_key = d.date_key
            WHERE f.order_date_key IS NOT NULL AND d.date_key IS NULL
        """,
        "expected": 0,
        "description": "Every non-null fact_orders.order_date_key must exist in gold.dim_date",
    },
    {
        "name": "gold_dim_payment_type_completeness",
        "sql": """
            SELECT COUNT(*) FROM silver.payments p
            LEFT JOIN gold.dim_payment_type pt ON p.payment_type = pt.payment_type
            WHERE pt.payment_type IS NULL
        """,
        "expected": 0,
        "description": "Every silver.payments.payment_type must exist in gold.dim_payment_type",
    },
    # ── Gold fact grain ───────────────────────────────────────────────────────
    {
        "name": "gold_fact_orders_unique_grain",
        "sql": """
            SELECT COUNT(*) FROM (
                SELECT order_id FROM gold.fact_orders
                GROUP BY order_id HAVING COUNT(*) > 1
            ) dups
        """,
        "expected": 0,
        "description": "gold.fact_orders: one row per order_id (grain must not be broken)",
    },
    {
        "name": "gold_fact_reviews_unique_grain",
        "sql": """
            SELECT COUNT(*) FROM (
                SELECT review_id FROM gold.fact_reviews
                GROUP BY review_id HAVING COUNT(*) > 1
            ) dups
        """,
        "expected": 0,
        "description": "gold.fact_reviews: one row per review_id",
    },
    # ── Gold fact business rules ──────────────────────────────────────────────
    {
        "name": "gold_no_negative_revenue",
        "sql": "SELECT COUNT(*) FROM gold.fact_orders WHERE gross_revenue < 0",
        "expected": 0,
        "description": "gold.fact_orders: gross_revenue must never be negative",
    },
    {
        "name": "gold_no_negative_payment_amounts",
        "sql": "SELECT COUNT(*) FROM gold.fact_payments WHERE total_amount < 0",
        "expected": 0,
        "description": "gold.fact_payments: total_amount must never be negative",
    },
    {
        "name": "gold_no_future_order_dates",
        "sql": "SELECT COUNT(*) FROM gold.fact_orders WHERE order_date_key > CURRENT_DATE",
        "expected": 0,
        "description": "gold.fact_orders: no orders should have a future purchase date",
    },
    {
        "name": "gold_fulfilment_days_non_negative",
        "sql": """
            SELECT COUNT(*) FROM gold.fact_orders
            WHERE fulfilment_days IS NOT NULL AND fulfilment_days < 0
        """,
        "expected": 0,
        "description": "gold.fact_orders: fulfilment_days must be ≥ 0 when set",
    },
    {
        "name": "gold_valid_sentiment_values",
        "sql": """
            SELECT COUNT(*) FROM gold.fact_reviews
            WHERE sentiment NOT IN ('positive', 'neutral', 'negative', 'unscored')
        """,
        "expected": 0,
        "description": "gold.fact_reviews: sentiment must be one of the four defined buckets",
    },
    {
        "name": "gold_valid_delivery_status_values",
        "sql": """
            SELECT COUNT(*) FROM gold.fact_orders
            WHERE delivery_status NOT IN ('on_time', 'late', 'pending')
        """,
        "expected": 0,
        "description": "gold.fact_orders: delivery_status must be on_time, late, or pending",
    },
    {
        "name": "gold_items_revenue_matches_orders",
        "sql": """
            SELECT COUNT(*) FROM (
                SELECT
                    fo.order_id,
                    fo.product_revenue,
                    COALESCE(SUM(fi.unit_price), 0)::numeric(14,2) AS item_sum
                FROM gold.fact_orders fo
                LEFT JOIN gold.fact_order_items fi ON fo.order_id = fi.order_id
                GROUP BY fo.order_id, fo.product_revenue
                HAVING ABS(fo.product_revenue - COALESCE(SUM(fi.unit_price), 0)) > 0.05
            ) mismatch
        """,
        "expected": 0,
        "description": "fact_orders.product_revenue must equal sum of fact_order_items.unit_price (tolerance ±0.05)",
    },
    # ── Analytics aggregate sanity ────────────────────────────────────────────
    {
        "name": "analytics_monthly_revenue_no_negatives",
        "sql": "SELECT COUNT(*) FROM analytics.monthly_revenue WHERE gross_revenue < 0",
        "expected": 0,
        "description": "analytics.monthly_revenue: gross_revenue must be non-negative",
    },
    {
        "name": "analytics_on_time_pct_in_range",
        "sql": """
            SELECT COUNT(*) FROM analytics.monthly_revenue
            WHERE on_time_pct NOT BETWEEN 0 AND 100
        """,
        "expected": 0,
        "description": "analytics.monthly_revenue: on_time_pct must be 0–100",
    },
    {
        "name": "analytics_customer_segments_valid_labels",
        "sql": """
            SELECT COUNT(*) FROM analytics.customer_segments
            WHERE segment NOT IN (
                'Champions', 'Loyal', 'Recent', 'Big Spenders',
                'At Risk', 'Lost', 'Occasional'
            )
        """,
        "expected": 0,
        "description": "analytics.customer_segments: all rows must have a known RFM segment label",
    },
    {
        "name": "analytics_rfm_scores_in_range",
        "sql": """
            SELECT COUNT(*) FROM analytics.customer_segments
            WHERE r_score NOT BETWEEN 1 AND 4
               OR f_score NOT BETWEEN 1 AND 4
               OR m_score NOT BETWEEN 1 AND 4
        """,
        "expected": 0,
        "description": "analytics.customer_segments: RFM scores must each be 1–4 (NTILE quartiles)",
    },
    {
        "name": "analytics_category_freight_pct_in_range",
        "sql": """
            SELECT COUNT(*) FROM analytics.category_performance
            WHERE freight_pct NOT BETWEEN 0 AND 100
        """,
        "expected": 0,
        "description": "analytics.category_performance: freight_pct must be 0–100",
    },
]

# ── BI view checks ────────────────────────────────────────────────────────────
# Verify every public.* view resolves without error and returns rows.

BI_VIEWS = [
    "public.dim_date",
    "public.dim_customer",
    "public.dim_product",
    "public.dim_seller",
    "public.dim_payment_type",
    "public.fact_orders",
    "public.fact_order_items",
    "public.fact_payments",
    "public.fact_reviews",
    "public.monthly_revenue",
    "public.category_performance",
    "public.seller_performance",
    "public.customer_segments",
]


def get_conn():
    return PostgresHook(postgres_conn_id=CONN_ID).get_conn()


# ── DAG ───────────────────────────────────────────────────────────────────────

@dag(
    dag_id="04_quality_checks_ecommerce",
    description="Validate bronze → silver → gold → analytics layers after nightly transforms",
    schedule="0 4 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "quality", "monitoring"],
)
def quality_checks_ecommerce():

    @task()
    def check_row_counts() -> dict[str, Any]:
        """
        Assert every table meets its minimum row threshold.
        Failures indicate a broken upstream DAG or data loss.
        """
        failures, results = [], {}
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                for table, minimum in ROW_COUNT_MINIMUMS.items():
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    actual = cur.fetchone()[0]
                    passed = actual >= minimum
                    results[table] = {"actual": actual, "minimum": minimum, "passed": passed}
                    if passed:
                        logger.info(f"  ✓ {table:<50} {actual:>10,} rows (≥ {minimum:,})")
                    else:
                        logger.error(f"  ✗ {table:<50} {actual:>10,} rows — expected ≥ {minimum:,}")
                        failures.append(table)
        finally:
            conn.close()
        if failures:
            raise ValueError(f"Row count check FAILED for: {failures}")
        return results

    @task()
    def check_null_rates() -> dict[str, Any]:
        """
        Assert null rates on key columns do not exceed defined thresholds.
        A spike in nulls often indicates a schema drift or a transform bug.
        """
        failures, results = [], {}
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                for (table, column, max_pct) in NULL_RATE_CHECKS:
                    cur.execute(f"""
                        SELECT COUNT(*) FILTER (WHERE "{column}" IS NULL)::float
                               / NULLIF(COUNT(*), 0)
                        FROM {table}
                    """)
                    null_rate = cur.fetchone()[0] or 0.0
                    passed = null_rate <= max_pct
                    key = f"{table}.{column}"
                    results[key] = {"null_pct": round(null_rate * 100, 3), "passed": passed}
                    if passed:
                        logger.info(f"  ✓ {key:<60} {null_rate * 100:>6.2f}% nulls")
                    else:
                        logger.error(
                            f"  ✗ {key:<60} {null_rate * 100:>6.2f}% nulls "
                            f"— max allowed {max_pct * 100:.1f}%"
                        )
                        failures.append(key)
        finally:
            conn.close()
        if failures:
            raise ValueError(f"Null rate check FAILED for: {failures}")
        return results

    @task()
    def check_business_rules() -> dict[str, Any]:
        """
        Assert domain-specific invariants across the full medallion stack.
        Each check runs a COUNT query; expected value is always 0 (no violations).
        """
        failures, results = [], {}
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                for check in BUSINESS_RULE_CHECKS:
                    cur.execute(check["sql"])
                    actual = cur.fetchone()[0]
                    passed = actual == check["expected"]
                    results[check["name"]] = {
                        "actual": actual,
                        "expected": check["expected"],
                        "passed": passed,
                    }
                    if passed:
                        logger.info(f"  ✓ {check['name']}")
                    else:
                        logger.error(
                            f"  ✗ {check['name']}: {actual} violations — {check['description']}"
                        )
                        failures.append(check["name"])
        finally:
            conn.close()
        if failures:
            raise ValueError(f"Business rule checks FAILED: {failures}")
        return results

    @task()
    def check_bi_views() -> dict[str, Any]:
        """
        Verify every public.* BI view is queryable and returns at least one row.
        A broken view means a BI tool will silently show an empty report.
        """
        failures, results = [], {}
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                for view in BI_VIEWS:
                    try:
                        cur.execute(f"SELECT COUNT(*) FROM {view}")
                        count = cur.fetchone()[0]
                        passed = count > 0
                        results[view] = {"rows": count, "passed": passed}
                        if passed:
                            logger.info(f"  ✓ {view:<45} {count:>10,} rows")
                        else:
                            logger.error(f"  ✗ {view}: view exists but returned 0 rows")
                            failures.append(view)
                    except Exception as exc:
                        logger.error(f"  ✗ {view}: query failed — {exc}")
                        failures.append(view)
                        conn.rollback()     # reset aborted transaction after error
        finally:
            conn.close()
        if failures:
            raise ValueError(f"BI view check FAILED for: {failures}")
        return results

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def quality_passed(row_counts, null_rates, business_rules, bi_views) -> None:
        total = (
            len(row_counts)
            + len(null_rates)
            + len(business_rules)
            + len(bi_views)
        )
        logger.info("=" * 60)
        logger.info("QUALITY GATE PASSED")
        logger.info("=" * 60)
        logger.info(f"  {total} checks — all green ✓")
        logger.info(f"    Row count checks  : {len(row_counts)}")
        logger.info(f"    Null rate checks  : {len(null_rates)}")
        logger.info(f"    Business rules    : {len(business_rules)}")
        logger.info(f"    BI view checks    : {len(bi_views)}")

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def quality_failed() -> None:
        logger.error("=" * 60)
        logger.error("QUALITY GATE FAILED — review task logs above.")
        logger.error("=" * 60)
        raise RuntimeError("Quality gate failed — downstream DAGs should not proceed.")

    rc = check_row_counts()
    nr = check_null_rates()
    br = check_business_rules()
    bv = check_bi_views()

    quality_passed(rc, nr, br, bv)
    [rc, nr, br, bv] >> quality_failed()


quality_checks_ecommerce()