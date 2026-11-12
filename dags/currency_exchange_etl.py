"""
Airflow ETL Pipeline: Daily Currency Exchange Rates to MongoDB
Orchestrated with Astro CLI.
Uses Frankfurter API, Named XCom for data transfer, and loads into MongoDB.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone, timedelta
import pendulum
import requests
from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------
# Configuration & Constants
# ---------------------------------------------------------
API_BASE_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_QUOTES = "USD,GBP,JPY"
SOURCE_NAME = "Frankfurter API"

# Named XCom Keys
XCOM_RAW_RATES_KEY = "raw_currency_rates"
XCOM_TRANSFORMED_RATES_KEY = "transformed_currency_rates"

# MongoDB Configuration
# In docker-compose, Airflow talks to MongoDB service via 'mongodb:27017'.
# Can be overridden via MONGO_URI environment variable.
DEFAULT_MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://admin:password123@mongodb:27017/?authSource=admin"
)
DATABASE_NAME = os.getenv("MONGO_DATABASE", "currency_db")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "exchange_rates")

logger = logging.getLogger("airflow.task")

# ---------------------------------------------------------
# Core ETL Helper Functions
# ---------------------------------------------------------
def transform_rates_payload(raw_data: list[dict], retrieval_timestamp: str | None = None) -> list[dict]:
    """
    Transforms raw Frankfurter API array of rates into the required schema:
    {
      "base_currency": "EUR",
      "target_currency": "USD",
      "exchange_rate": 1.15,
      "rate_date": "2026-11-04",
      "source": "Frankfurter API",
      "retrieved_at": "2026-11-05T07:15:00"
    }
    """
    if not isinstance(raw_data, list):
        raise ValueError(f"Expected a list of rate records from API, got {type(raw_data).__name__}")

    if retrieval_timestamp is None:
        retrieval_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    transformed = []
    for item in raw_data:
        base_currency = item.get("base")
        target_currency = item.get("quote")
        rate = item.get("rate")
        rate_date = item.get("date")

        if not all([base_currency, target_currency, rate is not None, rate_date]):
            logger.warning(f"Skipping malformed item: {item}")
            continue

        record = {
            "base_currency": str(base_currency).upper(),
            "target_currency": str(target_currency).upper(),
            "exchange_rate": float(rate),
            "rate_date": str(rate_date),
            "source": SOURCE_NAME,
            "retrieved_at": retrieval_timestamp,
        }
        transformed.append(record)

    if not transformed:
        raise ValueError("Transformation resulted in empty record set; please verify raw API input.")

    return transformed


# ---------------------------------------------------------
# Airflow Task Callables
# ---------------------------------------------------------
def extract_currency_rates(**context):
    """
    Task 1: Extract Currency Exchange Rates from Frankfurter API.
    Pushes raw response payload into Named XCom.
    """
    ti = context["ti"]
    url = f"{API_BASE_URL}?base={BASE_CURRENCY}&quotes={TARGET_QUOTES}"
    logger.info(f"Extracting exchange rates from API: {url}")

    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch exchange rates from API: {e}")
        raise

    logger.info(f"Successfully extracted {len(data)} items from Frankfurter API.")
    # Push to Named XCom
    ti.xcom_push(key=XCOM_RAW_RATES_KEY, value=data)


def transform_currency_rates(**context):
    """
    Task 2: Transform Exchange Rate Data.
    Pulls raw data from Named XCom, maps to required schema,
    and pushes transformed records to Named XCom.
    """
    ti = context["ti"]
    logger.info(f"Pulling raw data from Named XCom key: {XCOM_RAW_RATES_KEY}")
    raw_data = ti.xcom_pull(key=XCOM_RAW_RATES_KEY, task_ids="extract_exchange_rates")

    if not raw_data:
        raise ValueError(f"No data received from task 'extract_exchange_rates' for key '{XCOM_RAW_RATES_KEY}'.")

    transformed_records = transform_rates_payload(raw_data)
    logger.info(f"Successfully transformed {len(transformed_records)} exchange rate records.")
    for rec in transformed_records:
        logger.info(f"Sample transformed record: {rec}")

    # Push to Named XCom
    ti.xcom_push(key=XCOM_TRANSFORMED_RATES_KEY, value=transformed_records)


def load_to_mongodb(**context):
    """
    Task 3: Load Data into MongoDB.
    Pulls transformed data from Named XCom and upserts documents into MongoDB.
    """
    ti = context["ti"]
    logger.info(f"Pulling transformed records from Named XCom key: {XCOM_TRANSFORMED_RATES_KEY}")
    records = ti.xcom_pull(key=XCOM_TRANSFORMED_RATES_KEY, task_ids="transform_exchange_rates")

    if not records:
        raise ValueError(f"No records received from task 'transform_exchange_rates' for key '{XCOM_TRANSFORMED_RATES_KEY}'.")

    # Connect to MongoDB
    # If DEFAULT_MONGO_URI fails (e.g. host fallback during local tests), try localhost
    mongo_uris = [DEFAULT_MONGO_URI, "mongodb://admin:password123@localhost:27017/?authSource=admin"]
    client = None
    connected_uri = None

    for uri in mongo_uris:
        try:
            logger.info(f"Attempting MongoDB connection to: {uri.split('@')[-1]}")
            c = MongoClient(uri, serverSelectionTimeoutMS=4000)
            c.admin.command("ping")
            client = c
            connected_uri = uri
            logger.info(f"Connected successfully to MongoDB at {uri.split('@')[-1]}")
            break
        except Exception as e:
            logger.warning(f"Could not connect using {uri.split('@')[-1]}: {e}")

    if client is None:
        raise ConnectionError(f"Could not connect to MongoDB on any configured URI ({mongo_uris}).")

    try:
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        # Use bulk upserts based on (base_currency, target_currency, rate_date)
        operations = [
            UpdateOne(
                filter={
                    "base_currency": record["base_currency"],
                    "target_currency": record["target_currency"],
                    "rate_date": record["rate_date"],
                },
                update={"$set": record},
                upsert=True,
            )
            for record in records
        ]

        result = collection.bulk_write(operations)
        logger.info(
            f"MongoDB Load Complete into '{DATABASE_NAME}.{COLLECTION_NAME}': "
            f"matched={result.matched_count}, modified={result.modified_count}, upserted={len(result.upserted_ids)}"
        )
    except PyMongoError as e:
        logger.error(f"Error during MongoDB write operation: {e}")
        raise
    finally:
        client.close()


# ---------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="currency_exchange_etl",
    default_args=default_args,
    description="Daily Currency Exchange Rate ETL Pipeline to MongoDB via Astro CLI",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    tags=["etl", "currency", "mongodb", "assignment"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_exchange_rates",
        python_callable=extract_currency_rates,
    )

    transform_task = PythonOperator(
        task_id="transform_exchange_rates",
        python_callable=transform_currency_rates,
    )

    load_task = PythonOperator(
        task_id="load_to_mongodb",
        python_callable=load_to_mongodb,
    )

    # 3-task dependency pipeline
    extract_task >> transform_task >> load_task

