# Assignment Report: Building Airflow ETL Pipeline
**Course**: MLOps  
**Assignment**: Task 3 - Building Airflow ETL Pipeline  
**Student Name**: [Your Name]  
**Repository Link**: [GitHub / DagsHub URL]  

---

## 1. Executive Summary
This project implements an automated, containerized ETL pipeline using Apache Airflow orchestrated by the Astro CLI. The pipeline extracts daily currency exchange rates from the public Frankfurter API, applies a strict transformation schema, and persists records in MongoDB. The database is monitored and managed via Mongo Express.

---

## 2. Architecture & Pipeline Design
The ETL pipeline comprises three sequential tasks communicating through **Named XComs**:
1. `extract_exchange_rates`: Fetches raw quotes for EUR against USD, GBP, and JPY from Frankfurter API (`https://api.frankfurter.dev/v2/rates?base=EUR&quotes=USD,GBP,JPY`) and pushes the payload under XCom key `raw_currency_rates`.
2. `transform_exchange_rates`: Pulls `raw_currency_rates`, extracts the currency attributes, adds metadata (`source`, UTC timestamp `retrieved_at`), and pushes the shaped records under XCom key `transformed_currency_rates`.
3. `load_to_mongodb`: Pulls `transformed_currency_rates` and executes idempotent bulk upserts into MongoDB (`currency_db.exchange_rates`).

---

## 3. Infrastructure & Docker Setup
- **Docker Compose Override (`docker-compose.override.yml`)**: Extends Astro CLI runtime to spawn MongoDB (`mongo:7.0`) and Mongo Express (`mongo-express:latest`).
- **Persistent Storage**: Utilizes a named volume `mongodb_data:/data/db` to preserve database states across container lifecycles.
- **Docker Networking**: Integrated with Airflow's internal bridge network for DNS resolution (`mongodb:27017`).

---

## 4. Execution & Verification Screenshots

### Screenshot 1: Airflow DAG Graph & Grid View
*Insert screenshot showing `currency_exchange_etl` running successfully with all three tasks green.*

*(Screenshot Placeholder)*

---

### Screenshot 2: Airflow Named XCom Values
*Insert screenshot from Airflow UI (**Admin -> XComs**) showing `raw_currency_rates` and `transformed_currency_rates` entries.*

*(Screenshot Placeholder)*

---

### Screenshot 3: Mongo Express Database & Collection View
*Insert screenshot of Mongo Express at `http://localhost:8081` showing the database `currency_db` and collection `exchange_rates`.*

*(Screenshot Placeholder)*

---

### Screenshot 4: Transformed MongoDB Document Example
*Insert screenshot showing the JSON document inside Mongo Express matching the required schema:*
```json
{
  "_id": "...",
  "base_currency": "EUR",
  "target_currency": "USD",
  "exchange_rate": 1.1122,
  "rate_date": "2026-11-12",
  "source": "Frankfurter API",
  "retrieved_at": "2026-11-12T17:15:00"
}
```

---

## 5. Automated Testing Results
Unit tests were executed using `pytest` covering DAG structure, linear dependencies, schema validity, and Named XCom push/pull assertions:
```
tests/test_currency_exchange_etl.py ...... [100%]
6 passed in 0.73s
```

