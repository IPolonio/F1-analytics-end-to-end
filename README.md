# F1 Analytics

A data pipeline that collects Formula 1 race data from the [OpenF1 API](https://openf1.org/) into a PostgreSQL database, as a foundation for analyzing F1 sessions and driver performance.

## What it is

F1 Analytics is an ETL/analytics project. It runs scheduled Apache Airflow pipelines that pull data from the public OpenF1 API, normalize it, and store it in a PostgreSQL data warehouse. The goal is to build a dataset that can be used to analyze how drivers and teams perform across practice, qualifying, and race sessions.

## What problem it solves

Raw F1 data is scattered across the OpenF1 API, which is session-oriented and rate-limited. This project:

- Automates collection so data is continuously available instead of fetched on demand.
- Consolidates per-session API responses into queryable relational tables.
- Adds metadata (source and ingestion time) so rows are traceable.
- Centralizes everything in Postgres so analytics can run in SQL or any BI tool.

## Current scope

Two ingestion DAGs are implemented (both write to the `raw` schema, replacing table contents on each run):

| DAG | Source endpoint | Destination table | Content |
|---|---|---|---|
| `f1_meetings_ingestion` | `/v1/meetings?year=2025` | `raw.meetings_raw` | Race weekend metadata (circuit, dates, official names) |
| `f1_weather_ingestion` | `/v1/weather` per 2025 session | `raw.weather_raw` | Weather conditions per session (air temp, track temp, humidity, wind, rainfall) |

Driver performance analysis (lap times, position, telemetry, etc.) is the intended end use; the corresponding data feeds are not yet ingested.

## Architecture

```
OpenF1 API ──> Airflow DAGs ──> PostgreSQL (raw schema)
                  │
                  └─ retries + rate-limit handling
```

- **Orchestration:** Apache Airflow 3.3, LocalExecutor, SQLite metastore.
- **Extraction:** Python `urllib` requests to the OpenF1 API.
- **Loading:** pandas `to_sql` via the Airflow Postgres hook.
- **Source handling:** the OpenF1 API rate-limits requests (HTTP 429); the weather DAG retries with exponential backoff and sleeps 0.5s between requests.

## Requirements

- Python 3.14 (managed with `uv`)
- A running PostgreSQL database with a target schema `raw`
- Airflow, configured with a Postgres connection named `conn_postgres`

## Setup

```sh
uv sync
export AIRFLOW_HOME="$PWD/airflow"
airflow standalone
```

Register the Postgres connection and unpause the DAGs in the Airflow UI (DAGs are paused at creation by default). See `AGENTS.md` for full details on configuration and verification.

## Roadmap

- Ingest driver, session, and lap-time data from the OpenF1 API.
- Add transform/analytics layers (e.g. `marts` schema) on top of the raw tables.
- Model driver performance across sessions, teams, and conditions.
