"""
plugins/db.py
─────────────
Shared warehouse connection utilities used across all DAGs.
Compatible with SQLAlchemy 1.4.x (bundled in Astro Runtime).
Reads credentials from environment variables set in .env
"""
import os
import logging
from contextlib import contextmanager

import psycopg2
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)


def get_warehouse_conn_str() -> str:
    """Build SQLAlchemy connection string from environment variables."""
    host = os.environ["WAREHOUSE_HOST"]
    port = os.environ.get("WAREHOUSE_PORT", "5432")
    db   = os.environ["WAREHOUSE_DB"]
    user = os.environ["WAREHOUSE_USER"]
    pwd  = os.environ["WAREHOUSE_PASSWORD"]
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def get_engine():
    """
    Return a SQLAlchemy 1.4-compatible engine for the data warehouse.
    future=True opts into the 2.0-style connection behaviour while
    staying on the 1.4 library — avoids deprecation warnings.
    """
    return create_engine(
        get_warehouse_conn_str(),
        pool_pre_ping=True,
        future=True,          # SQLAlchemy 1.4 compatibility bridge
    )


@contextmanager
def get_psycopg2_conn():
    """
    Context manager yielding a raw psycopg2 connection.
    Commits on clean exit, rolls back on exception.
    Use this for DDL-heavy operations (CREATE, DROP, TRUNCATE).
    """
    host = os.environ["WAREHOUSE_HOST"]
    port = os.environ.get("WAREHOUSE_PORT", "5432")
    db   = os.environ["WAREHOUSE_DB"]
    user = os.environ["WAREHOUSE_USER"]
    pwd  = os.environ["WAREHOUSE_PASSWORD"]

    conn = psycopg2.connect(
        host=host, port=port, dbname=db, user=user, password=pwd
    )
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def run_sql(sql: str) -> None:
    """
    Execute a raw SQL string via psycopg2 (DDL-safe, autocommit-friendly).
    Preferred over SQLAlchemy engine.execute() which is removed in SA 2.0.
    """
    with get_psycopg2_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
    logger.info("SQL executed successfully")