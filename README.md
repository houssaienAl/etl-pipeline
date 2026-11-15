# End-to-End ETL Pipeline

An **Apache Airflow** DAG that ingests three kinds of sources (REST API, CSV, JSON), cleans and joins them with
**PySpark**, loads **PostgreSQL**, then runs data quality checks. Everything runs in **Docker Compose**.

```
orders.csv ────────┐
customers.json ────┼─► spark_transform_load ─► PostgreSQL ─► quality_checks
FX rates REST API ─┘     (clean, dedupe, join,    dim_customers
                          convert to EUR)         fact_orders
```

## What it does

| Task | Description |
|---|---|
| `land_source_files` | Stand-in for upstream exports: writes a deliberately dirty `orders.csv` and `customers.json` |
| `extract_fx_rates_api` | Pulls daily FX rates from the [Frankfurter](https://frankfurter.dev) REST API |
| `spark_transform_load` | PySpark: normalises fields, drops invalid rows, dedupes, removes orphan orders, converts amounts to EUR, bulk-loads Postgres over JDBC |
| `quality_checks` | 7 SQL checks on the loaded tables (non-empty, no orphans, positive amounts, valid statuses, ...); any failure fails the run |

**Alerting**: every task has an `on_failure_callback` ([`etl/alerts.py`](etl/alerts.py)) that logs the failure and,
if `ALERT_WEBHOOK_URL` is set, posts it to a Slack-compatible webhook. Tasks retry twice before alerting.

**Idempotent**: each run truncates and reloads the target tables, so reruns never duplicate data.

## Run it

```bash
docker compose up -d --build
```

Open http://localhost:8080 (user `admin`, password `admin`, local demo only), unpause and trigger `etl_pipeline`.
Or from the command line:

```bash
docker compose exec airflow airflow dags test etl_pipeline
```

Check the result:

```bash
docker compose exec postgres psql -U etl -d warehouse -c "select status, count(*), sum(amount_eur) from fact_orders group by 1"
```

### Data volume

The generator writes 1,000,000 orders by default so the demo runs on a laptop. The size is a setting:

```bash
N_ORDERS=10000000 SPARK_DRIVER_MEMORY=4g docker compose up -d
```

(delete `data/orders.csv` first to regenerate). Measured on a 16-thread laptop: 10.1M raw rows in,
9.85M loaded, 250k rejected by the cleaning rules, all quality checks green, about 2.5 minutes end to end. Spark runs in `local[*]` mode inside the Airflow container;
set `SPARK_MASTER=spark://host:7077` to submit to a cluster instead.

### Tests

```bash
docker compose exec airflow python tests/test_transform.py
```

## Layout

```
dags/etl_pipeline.py   DAG definition
etl/                   generate_data, extract_api, transform (PySpark), quality, alerts
sql/init.sql           warehouse schema
tests/                 cleaning-rule self-check
```
