"""
dags/01_ingest_raw.py
──────────────────────
DAG 1 — ingest_ecommerce_raw

Reads Olist CSV files and loads them into the bronze schema using
PostgresHook.get_conn() (psycopg2) + COPY for fast bulk inserts.

Prerequisite — create this connection in Airflow UI:
  Admin → Connections → +
  conn_id   : postgres_warehouse_local
  conn_type : Postgres
  host      : localhost
  database  : ecommerce_dw
  login     : postgres
  password  : kanda
  port      : 3001

Place Olist CSVs in: include/data/

Schedule: daily at 01:00 UTC
"""
from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

CONN_ID  = "postgres_warehouse_local"
DATA_DIR = Path(os.environ.get("DATA_DIR", "/usr/local/airflow/include/data"))

RAW_TABLES = {
    "bronze.orders":          "olist_orders_dataset.csv",
    "bronze.order_items":     "olist_order_items_dataset.csv",
    "bronze.customers":       "olist_customers_dataset.csv",
    "bronze.products":        "olist_products_dataset.csv",
    "bronze.sellers":         "olist_sellers_dataset.csv",
    "bronze.payments":        "olist_order_payments_dataset.csv",
    "bronze.reviews":         "olist_order_reviews_dataset.csv",
    "bronze.geolocation":     "olist_geolocation_dataset.csv",
    "bronze.product_category_translation": "product_category_name_translation.csv",
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def get_conn():
    hook = PostgresHook(postgres_conn_id=CONN_ID)
    return hook.get_conn()


@dag(
    dag_id="01_ingest_ecommerce_raw",
    description="Load Olist CSV files into bronze schema",
    schedule="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["ecommerce", "ingest", "bronze"],
)
def ingest_ecommerce_raw():

    @task()
    def validate_source_files() -> dict:
        report, missing = {}, []
        for table, filename in RAW_TABLES.items():
            filepath = DATA_DIR / filename
            if not filepath.exists():
                missing.append(filename)
                continue
            size = filepath.stat().st_size
            report[table] = {"path": str(filepath), "size_bytes": size}
            logger.info(f"  ✓ {filename} ({size:,} bytes)")
        if missing:
            raise FileNotFoundError(
                f"Missing source files: {missing}\n"
                f"Place the Olist CSVs in: {DATA_DIR}"
            )
        logger.info(f"All {len(report)} source files validated.")
        return report

    @task()
    def create_bronze_schema() -> None:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA IF NOT EXISTS bronze")
            conn.commit()
            logger.info("Bronze schema ready!!")
        finally:
            conn.close()

    @task()
    def load_bronze_table(table_name: str, filename: str) -> dict:
        filepath = DATA_DIR / filename
        logger.info(f"Loading {filepath} → {table_name}")

        df = pd.read_csv(filepath, dtype=str, keep_default_na=False)
        df["_loaded_at"] = datetime.utcnow().isoformat()
        row_count = len(df)

        schema, tbl = table_name.split(".")
        col_defs = ",\n  ".join(f'"{c}" TEXT' for c in df.columns)
        create_sql = f'DROP TABLE IF EXISTS {table_name} CASCADE;\nCREATE TABLE {table_name} (\n  {col_defs}\n)'
        copy_sql   = f'COPY {table_name} ({", ".join(f"{chr(34)}{c}{chr(34)}" for c in df.columns)}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, QUOTE \'"\')'

        buffer = io.StringIO()
        df.to_csv(buffer, index=False, quoting=csv.QUOTE_ALL)
        buffer.seek(0)

        conn = get_conn()
        try:
            with conn.cursor() as cur:
                for stmt in [s.strip() for s in create_sql.split(";") if s.strip()]:
                    cur.execute(stmt)
                conn.commit()
            with conn.cursor() as cur:
                cur.copy_expert(copy_sql, buffer)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        logger.info(f"  → {row_count:,} rows loaded into {table_name}")
        return {"table": table_name, "rows": row_count, "file": filename}

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def ingest_summary(results: list[dict]) -> None:
        total = sum(r["rows"] for r in results)
        logger.info("=" * 55)
        logger.info("INGEST COMPLETE")
        logger.info("=" * 55)
        for r in results:
            logger.info(f"  {r['table']:<45} {r['rows']:>8,} rows")
        logger.info("-" * 55)
        logger.info(f"  {'TOTAL':<45} {total:>8,} rows")

    validation    = validate_source_files()
    schema_task   = create_bronze_schema()

    load_tasks = [
        load_bronze_table.override(task_id=f"load_{tbl.split('.')[1]}")(
            table_name=tbl, filename=fname
        )
        for tbl, fname in RAW_TABLES.items()
    ]

    validation >> schema_task >> load_tasks
    ingest_summary(load_tasks)


ingest_ecommerce_raw()