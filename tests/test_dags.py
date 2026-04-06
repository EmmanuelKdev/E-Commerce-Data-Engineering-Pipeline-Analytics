"""
tests/test_dags.py
───────────────────
Basic DAG integrity tests — these run WITHOUT a live Airflow instance.
They check that DAGs parse correctly, have the right task IDs, and
that dependencies are wired as expected.

Run with:  pytest tests/
"""
import sys
import os
import pytest

# Put plugins on the path so db.py can be imported (mocked below)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "plugins"))

# ── Mock the db module so tests don't need a real DB ─────────────────────────
from unittest.mock import MagicMock, patch
import types

db_mock = types.ModuleType("db")
db_mock.get_engine = MagicMock()
db_mock.get_psycopg2_conn = MagicMock()
sys.modules["db"] = db_mock


# ── DAG import tests ──────────────────────────────────────────────────────────

class TestIngestDAG:
    def setup_method(self):
        import importlib.util, pathlib
        spec = importlib.util.spec_from_file_location(
            "dag01",
            pathlib.Path(__file__).parents[1] / "dags" / "01_ingest_raw.py",
        )
        self.mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.mod)

    def test_dag_exists(self):
        from airflow.models import DagBag
        import pathlib
        bag = DagBag(
            dag_folder=str(pathlib.Path(__file__).parents[1] / "dags"),
            include_examples=False,
        )
        assert "01_ingest_ecommerce_raw" in bag.dag_ids

    def test_no_import_errors(self):
        from airflow.models import DagBag
        import pathlib
        bag = DagBag(
            dag_folder=str(pathlib.Path(__file__).parents[1] / "dags"),
            include_examples=False,
        )
        assert bag.import_errors == {}


class TestTransformDAG:
    def test_dag_loads(self):
        from airflow.models import DagBag
        import pathlib
        bag = DagBag(
            dag_folder=str(pathlib.Path(__file__).parents[1] / "dags"),
            include_examples=False,
        )
        assert "02_transform_ecommerce" in bag.dag_ids

    def test_expected_task_ids(self):
        from airflow.models import DagBag
        import pathlib
        bag = DagBag(
            dag_folder=str(pathlib.Path(__file__).parents[1] / "dags"),
            include_examples=False,
        )
        dag = bag.get_dag("02_transform_ecommerce")
        task_ids = {t.task_id for t in dag.tasks}
        expected = {
            "stage_orders",
            "stage_order_items",
            "stage_customers",
            "stage_products",
            "stage_payments",
            "build_dim_customers",
            "build_dim_products",
            "build_dim_sellers",
            "build_dim_dates",
            "build_fact_orders",
            "build_fact_payments",
            "transform_complete",
        }
        assert expected.issubset(task_ids), f"Missing tasks: {expected - task_ids}"


class TestQualityDAG:
    def test_dag_loads(self):
        from airflow.models import DagBag
        import pathlib
        bag = DagBag(
            dag_folder=str(pathlib.Path(__file__).parents[1] / "dags"),
            include_examples=False,
        )
        assert "03_quality_checks_ecommerce" in bag.dag_ids

    def test_quality_task_ids(self):
        from airflow.models import DagBag
        import pathlib
        bag = DagBag(
            dag_folder=str(pathlib.Path(__file__).parents[1] / "dags"),
            include_examples=False,
        )
        dag = bag.get_dag("03_quality_checks_ecommerce")
        task_ids = {t.task_id for t in dag.tasks}
        assert "check_row_counts"     in task_ids
        assert "check_null_rates"     in task_ids
        assert "check_business_rules" in task_ids
        assert "quality_passed"       in task_ids
        assert "quality_failed"       in task_ids


# ── SQL logic unit tests (no DB needed) ───────────────────────────────────────

class TestRowCountConfig:
    def test_all_analytics_tables(self):
        """All tables in ROW_COUNT_MINIMUMS must be in analytics schema."""
        import pathlib, importlib.util
        spec = importlib.util.spec_from_file_location(
            "dag03",
            pathlib.Path(__file__).parents[1] / "dags" / "03_quality_checks.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        for table in mod.ROW_COUNT_MINIMUMS:
            assert table.startswith("analytics."), \
                f"{table} should be in analytics schema"

    def test_minimums_are_positive(self):
        import pathlib, importlib.util
        spec = importlib.util.spec_from_file_location(
            "dag03b",
            pathlib.Path(__file__).parents[1] / "dags" / "03_quality_checks.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        for table, minimum in mod.ROW_COUNT_MINIMUMS.items():
            assert minimum > 0, f"{table} minimum should be > 0"

    def test_null_thresholds_in_range(self):
        import pathlib, importlib.util
        spec = importlib.util.spec_from_file_location(
            "dag03c",
            pathlib.Path(__file__).parents[1] / "dags" / "03_quality_checks.py",
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        for (_, col, pct) in mod.NULL_RATE_CHECKS:
            assert 0.0 <= pct <= 1.0, \
                f"{col} threshold {pct} must be between 0 and 1"
