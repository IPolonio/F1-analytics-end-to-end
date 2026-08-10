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
OpenF1 API ──> Airflow DAGs ──> PostgreSQL (raw schema) ──> dbt ──> PostgreSQL (staging + drivers_gold schemas)
                  │                                           │
                  └─ retries + rate-limit handling            └─ tests, docs, lineage
```

- **Orchestration:** Apache Airflow 3.3, LocalExecutor, SQLite metastore.
- **Extraction:** Python `urllib` requests to the OpenF1 API.
- **Loading:** pandas `to_sql` via the Airflow Postgres hook.
- **Source handling:** the OpenF1 API rate-limits requests (HTTP 429); the weather DAG retries with exponential backoff and sleeps 0.5s between requests.
- **Transformation:** dbt Core with the Postgres adapter, materializing models as views.

## Current scope

The `raw` schema holds four OpenF1 tables (replaced on each run):

| Raw table | Source endpoint | Content |
|---|---|---|
| `raw.meetings_raw` | `/v1/meetings?year=2025` | Race weekend metadata (circuit, dates, official names) |
| `raw.sessions_raw` | `/v1/sessions?year=2025` | Session metadata (type, dates, circuit, meeting reference) |
| `raw.drivers_raw` | `/v1/drivers` | Driver registrations per session (identity, team, broadcast names) |
| `raw.weather_raw` | `/v1/weather` per 2025 session | Weather conditions per session (air temp, track temp, humidity, wind, rainfall) |

The repo ships two ingestion DAGs (`f1_meetings_ingestion`, `f1_weather_ingestion`); both write to the `raw` schema, replacing table contents on each run. Driver performance analysis (lap times, position, telemetry, etc.) is the intended end use; those data feeds are not yet ingested.

## dbt project

The dbt project lives in `dbt/` and models the raw tables into two layers:

```
dbt/
├── dbt_project.yml          # project config (profiles, model paths, materializations)
├── profiles.yml             # Postgres connection (env-var driven; configure before first run)
├── macros/
│   └── generate_schema_name.sql  # keep configured schema names (no base-schema prefix)
└── models/
    ├── sources.yml          # definitions of the raw schema tables
    ├── schema.yml           # tests + column docs for every model
    ├── staging/             # cleaned, typed views on the raw tables
    │   ├── stg_meetings.sql
    │   ├── stg_sessions.sql
    │   ├── stg_drivers.sql
    │   └── stg_weather.sql
    └── marts/               # analytical views for reporting / BI
        ├── dim_meetings.sql
        ├── dim_sessions.sql
        ├── dim_drivers.sql
        ├── fct_weather.sql
        ├── fct_driver_sessions.sql
        └── rpt_weather_by_session.sql
```

| Model | Schema | Purpose |
|---|---|---|
| `stg_meetings` | `staging` | Race weekend metadata with proper types (`meeting_key`, `date_start`, `year`) |
| `stg_sessions` | `staging` | Session metadata with typed dates, typed against `meeting_key` |
| `stg_drivers` | `staging` | Per-session driver registrations (identity, team, broadcast names) |
| `stg_weather` | `staging` | Typed weather readings plus an `is_raining` flag |
| `dim_meetings` | `drivers_gold` | Deduplicated dimension, one row per race weekend |
| `dim_sessions` | `drivers_gold` | One row per session, enriched with meeting info |
| `dim_drivers` | `drivers_gold` | Deduplicated driver dimension, one row per `driver_number` |
| `fct_weather` | `drivers_gold` | Fact table, one row per weather observation |
| `fct_driver_sessions` | `drivers_gold` | Driver appearances per session with team + meeting context |
| `rpt_weather_by_session` | `drivers_gold` | Per-session weather summary (averages, min/max temps, rainfall share) |

All models are materialized as **views**, so the marts always reflect the latest raw data. The `drivers_gold` schema is the analytics layer — ready to be queried in SQL or consumed by any BI tool. Example:

```sql
-- Which sessions had the hottest track conditions, and who was driving?
select
    s.circuit_short_name,
    s.session_name,
    w.avg_track_temperature,
    f.driver_number,
    f.full_name,
    f.team_name
from drivers_gold.rpt_weather_by_session w
left join drivers_gold.fct_driver_sessions f on f.session_key = w.session_key
left join drivers_gold.dim_sessions s on s.session_key = w.session_key
order by w.avg_track_temperature desc
limit 10;
```

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
uv run dbt parse --project-dir dbt --profiles-dir dbt

# Build the staging and analytical views (creates the staging + drivers_gold schemas)
uv run dbt build --project-dir dbt --profiles-dir dbt

# Run data quality tests only
uv run dbt test --project-dir dbt --profiles-dir dbt

# Connection sanity check
uv run dbt debug --project-dir dbt --profiles-dir dbt
```

Run Airflow first so the `raw` tables exist, then build the dbt models on top.

## Roadmap

- Ingest lap-time and telemetry data from the OpenF1 API.
- Model driver performance across sessions, teams, and conditions on top of the existing dimensions/facts.
- Add row-level lineage via dbt docs (`uv run dbt docs generate && uv run dbt docs serve`).
- Schedule dbt runs from Airflow instead of the CLI.
