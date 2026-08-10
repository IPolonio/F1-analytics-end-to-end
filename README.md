# F1 Analytics

A data pipeline that collects Formula 1 race data from the [OpenF1 API](https://openf1.org/) into a PostgreSQL database and transforms it into analytical views with dbt, as a foundation for analyzing F1 sessions, driver performance, and race conditions.

## What it is

F1 Analytics is an ETL/analytics project. It runs scheduled Apache Airflow pipelines that pull data from the public OpenF1 API, normalize it, and store it in a PostgreSQL data warehouse. A [dbt](https://docs.getdbt.com/) project then models that raw data into cleaned staging views and analytical marts that can be queried in SQL or consumed by any BI tool.

## What problem it solves

Raw F1 data is scattered across the OpenF1 API, which is session-oriented and rate-limited. This project:

- Automates collection so data is continuously available instead of fetched on demand.
- Consolidates per-session API responses into queryable relational tables.
- Adds metadata (source and ingestion time) so rows are traceable.
- Centralizes everything in Postgres so analytics can run in SQL or any BI tool.
- Uses dbt to codify transformations as version-controlled, testable, documented models instead of one-off SQL scripts.

## Architecture

```
OpenF1 API ──> Airflow DAGs ──> PostgreSQL (raw schema) ──> dbt ──> PostgreSQL (staging + marts schemas)
                  │                                           │
                  └─ retries + rate-limit handling            └─ tests, docs, lineage
```

- **Orchestration:** Apache Airflow 3.3, LocalExecutor, SQLite metastore.
- **Extraction:** Python `urllib` requests to the OpenF1 API.
- **Loading:** pandas `to_sql` via the Airflow Postgres hook.
- **Source handling:** the OpenF1 API rate-limits requests (HTTP 429); the weather DAG retries with exponential backoff and sleeps 0.5s between requests.
- **Transformation:** dbt Core with the Postgres adapter, materializing models as views.

## Current scope

Two ingestion DAGs are implemented (both write to the `raw` schema, replacing table contents on each run):

| DAG | Source endpoint | Destination table | Content |
|---|---|---|---|
| `f1_meetings_ingestion` | `/v1/meetings?year=2025` | `raw.meetings_raw` | Race weekend metadata (circuit, dates, official names) |
| `f1_weather_ingestion` | `/v1/weather` per 2025 session | `raw.weather_raw` | Weather conditions per session (air temp, track temp, humidity, wind, rainfall) |

Driver performance analysis (lap times, position, telemetry, etc.) is the intended end use; the corresponding data feeds are not yet ingested.

## dbt project

The dbt project lives in `dbt/` and models the raw tables into two layers:

```
dbt/
├── dbt_project.yml          # project config (profiles, model paths, materializations)
├── profiles.yml             # Postgres connection (env-var driven; configure before first run)
└── models/
    ├── sources.yml          # definitions of the raw schema tables
    ├── schema.yml           # tests + column docs for every model
    ├── staging/             # cleaned, typed views on the raw tables
    │   ├── stg_meetings.sql
    │   └── stg_weather.sql
    └── marts/               # analytical views for reporting / BI
        ├── dim_meetings.sql
        ├── fct_weather.sql
        └── rpt_weather_by_session.sql
```

| Model | Schema | Purpose |
|---|---|---|
| `stg_meetings` | `staging` | Race weekend metadata with proper types (`meeting_key`, `date_start`, `year`) |
| `stg_weather` | `staging` | Typed weather readings plus an `is_raining` flag |
| `dim_meetings` | `marts` | Deduplicated dimension, one row per race weekend |
| `fct_weather` | `marts` | Fact table, one row per weather observation |
| `rpt_weather_by_session` | `marts` | Per-session weather summary (averages, min/max temps, rainfall share) |

All models are materialized as **views**, so the marts always reflect the latest raw data.

## Requirements

- Python 3.14 (managed with `uv`)
- A running PostgreSQL database with a `raw` schema
- Airflow, configured with a Postgres connection named `conn_postgres`
- dbt Core + the Postgres adapter (installed via `uv sync`)

## Setup

```sh
uv sync
export AIRFLOW_HOME="$PWD/airflow"
airflow standalone
```

Register the Postgres connection and unpause the DAGs in the Airflow UI (DAGs are paused at creation by default). See `AGENTS.md` for full details on configuration and verification.

### Running dbt

dbt connects to the same Postgres database. `dbt/profiles.yml` is a template driven by environment variables (`POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `DBT_SCHEMA`) with local defaults — adjust it to match your setup before the first run.

```sh
# Validate the project and models
dbt parse --project-dir dbt --profiles-dir dbt

# Build the staging and marts views (creates staging + marts schemas)
dbt build --project-dir dbt --profiles-dir dbt

# Run data quality tests only
dbt test --project-dir dbt --profiles-dir dbt
```

Run Airflow first so the `raw` tables exist, then build the dbt models on top.

## Roadmap

- Ingest driver, session, and lap-time data from the OpenF1 API.
- Add sessions and drivers as staging/dimension models once those feeds land.
- Model driver performance across sessions, teams, and conditions.
- Schedule dbt runs from Airflow instead of the CLI.
