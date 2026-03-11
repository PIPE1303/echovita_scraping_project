"""Tests for the Echovita pipeline DAG. Run with: pytest tests/ -v"""
import sys
from pathlib import Path

import pytest

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))


def test_echovita_dag_loaded():
    from airflow.models import DagBag

    dag_bag = DagBag(dag_folder=str(_root / "dags"), include_examples=False)
    assert "echovita_pipeline" in dag_bag.dags
    dag = dag_bag.dags["echovita_pipeline"]
    task_ids = {t.task_id for t in dag.tasks}
    assert "run_scrapy_spider" in task_ids
    assert "validate_jsonl_export" in task_ids
    assert "run_consolidation" in task_ids


def test_echovita_dag_no_import_errors():
    from airflow.models import DagBag

    dag_bag = DagBag(dag_folder=str(_root / "dags"), include_examples=False)
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"
