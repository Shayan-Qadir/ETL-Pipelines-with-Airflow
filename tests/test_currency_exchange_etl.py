"""
Unit tests for Currency Exchange ETL DAG
Tests transformation schema, XCom configurations, and DAG structure.
Supports running inside Astro runtime as well as locally on host without full Airflow.
"""

import sys
import os
from unittest.mock import MagicMock, patch
import pytest

# Ensure dags directory is importable
DAGS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "dags")
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

# Mock airflow if not present in current python environment (e.g. host testing outside Docker)
try:
    import airflow
except ImportError:
    mock_airflow = MagicMock()
    mock_operators = MagicMock()

    _current_dag = None

    class MockDAG:
        def __init__(self, dag_id, default_args=None, schedule=None, start_date=None, catchup=False, tags=None, **kwargs):
            self.dag_id = dag_id
            self.default_args = default_args or {}
            self.schedule = schedule
            self.start_date = start_date
            self.catchup = catchup
            self.tags = tags or []
            self.tasks = []

        def __enter__(self):
            global _current_dag
            _current_dag = self
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            global _current_dag
            _current_dag = None

        def get_task(self, task_id):
            for t in self.tasks:
                if t.task_id == task_id:
                    return t
            raise KeyError(f"Task {task_id} not found")

    class MockOperator:
        def __init__(self, task_id, python_callable=None, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.downstream_list = []
            self.upstream_list = []
            if _current_dag is not None:
                _current_dag.tasks.append(self)

        def __rshift__(self, other):
            if isinstance(other, MockOperator):
                self.downstream_list.append(other)
                other.upstream_list.append(self)
            return other

    mock_airflow.DAG = MockDAG
    mock_operators.PythonOperator = MockOperator
    sys.modules["airflow"] = mock_airflow
    sys.modules["airflow.operators"] = mock_operators
    sys.modules["airflow.operators.python"] = mock_operators

from currency_exchange_etl import (
    dag,
    transform_rates_payload,
    XCOM_RAW_RATES_KEY,
    XCOM_TRANSFORMED_RATES_KEY,
    extract_currency_rates,
    transform_currency_rates,
    load_to_mongodb,
)


def test_dag_structure():
    """Verify DAG structure, task count, and linear dependencies."""
    assert dag.dag_id == "currency_exchange_etl"
    assert dag.tags == ["etl", "currency", "mongodb", "assignment"]
    assert dag.default_args.get("retries") >= 2

    task_ids = [t.task_id for t in dag.tasks]
    # In live airflow or mock, verify the 3 tasks
    extract_task = dag.get_task("extract_exchange_rates")
    transform_task = dag.get_task("transform_exchange_rates")
    load_task = dag.get_task("load_to_mongodb")

    assert extract_task is not None
    assert transform_task is not None
    assert load_task is not None

    # Verify dependency chain: extract >> transform >> load
    assert transform_task in extract_task.downstream_list
    assert load_task in transform_task.downstream_list


def test_transform_rates_payload_conforms_to_schema():
    """Test transformation function outputs the exact required schema."""
    raw_api_sample = [
        {"date": "2026-11-04", "base": "EUR", "quote": "USD", "rate": 1.15},
        {"date": "2026-11-04", "base": "EUR", "quote": "GBP", "rate": 0.84},
        {"date": "2026-11-04", "base": "EUR", "quote": "JPY", "rate": 175.5},
    ]

    transformed = transform_rates_payload(raw_api_sample, retrieval_timestamp="2026-11-05T07:15:00")

    assert len(transformed) == 3
    first = transformed[0]

    # Required schema check:
    # {
    #   "base_currency": "EUR",
    #   "target_currency": "USD",
    #   "exchange_rate": 1.15,
    #   "rate_date": "2026-11-04",
    #   "source": "Frankfurter API",
    #   "retrieved_at": "2026-11-05T07:15:00"
    # }
    assert first["base_currency"] == "EUR"
    assert first["target_currency"] == "USD"
    assert first["exchange_rate"] == 1.15
    assert first["rate_date"] == "2026-11-04"
    assert first["source"] == "Frankfurter API"
    assert first["retrieved_at"] == "2026-11-05T07:15:00"

    for record in transformed:
        assert isinstance(record["base_currency"], str)
        assert isinstance(record["target_currency"], str)
        assert isinstance(record["exchange_rate"], float)
        assert isinstance(record["rate_date"], str)
        assert record["source"] == "Frankfurter API"
        assert isinstance(record["retrieved_at"], str)


def test_transform_rates_payload_handles_empty_or_invalid():
    """Verify error handling for invalid payloads."""
    with pytest.raises(ValueError, match="Expected a list"):
        transform_rates_payload({"error": "invalid format"})

    with pytest.raises(ValueError, match="empty record set"):
        transform_rates_payload([])


def test_extract_task_pushes_named_xcom():
    """Verify Task 1 pushes data with the designated Named XCom key."""
    ti_mock = MagicMock()
    mock_response = MagicMock()
    mock_response.json.return_value = [
        {"date": "2026-11-12", "base": "EUR", "quote": "USD", "rate": 1.1122}
    ]
    mock_response.raise_for_status.return_value = None

    with patch("requests.get", return_value=mock_response):
        extract_currency_rates(ti=ti_mock)

    ti_mock.xcom_push.assert_called_once_with(
        key=XCOM_RAW_RATES_KEY,
        value=mock_response.json.return_value
    )


def test_transform_task_uses_named_xcom():
    """Verify Task 2 pulls and pushes using designated Named XCom keys."""
    ti_mock = MagicMock()
    ti_mock.xcom_pull.return_value = [
        {"date": "2026-11-12", "base": "EUR", "quote": "USD", "rate": 1.1122}
    ]

    transform_currency_rates(ti=ti_mock)

    ti_mock.xcom_pull.assert_called_once_with(
        key=XCOM_RAW_RATES_KEY,
        task_ids="extract_exchange_rates"
    )

    ti_mock.xcom_push.assert_called_once()
    call_kwargs = ti_mock.xcom_push.call_args[1]
    assert call_kwargs["key"] == XCOM_TRANSFORMED_RATES_KEY
    records = call_kwargs["value"]
    assert len(records) == 1
    assert records[0]["target_currency"] == "USD"
    assert records[0]["exchange_rate"] == 1.1122


def test_load_task_pulls_named_xcom_and_writes_to_mongo():
    """Verify Task 3 pulls named XCom and calls MongoDB bulk_write."""
    ti_mock = MagicMock()
    sample_transformed = [
        {
            "base_currency": "EUR",
            "target_currency": "USD",
            "exchange_rate": 1.1122,
            "rate_date": "2026-11-12",
            "source": "Frankfurter API",
            "retrieved_at": "2026-11-12T12:00:00",
        }
    ]
    ti_mock.xcom_pull.return_value = sample_transformed

    mock_client = MagicMock()
    mock_collection = MagicMock()
    mock_client.__getitem__.return_value.__getitem__.return_value = mock_collection
    mock_result = MagicMock()
    mock_result.matched_count = 1
    mock_result.modified_count = 0
    mock_result.upserted_ids = {}
    mock_collection.bulk_write.return_value = mock_result

    with patch("currency_exchange_etl.MongoClient", return_value=mock_client):
        load_to_mongodb(ti=ti_mock)

    ti_mock.xcom_pull.assert_called_once_with(
        key=XCOM_TRANSFORMED_RATES_KEY,
        task_ids="transform_exchange_rates"
    )
    mock_collection.bulk_write.assert_called_once()
