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
| `raw.positions_raw` | `/v1/position` per 2025 session | Running position per driver per lap — final reading gives the race result |

The repo ships three ingestion DAGs (`f1_meetings_ingestion`, `f1_weather_ingestion`, `f1_positions_ingestion`); they write to the `raw` schema, replacing table contents on each run. Driver performance analysis (lap times, telemetry, etc.) is the intended end use; those data feeds are not yet ingested.

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
    │   ├── stg_weather.sql
    │   └── stg_positions.sql
    └── marts/               # analytical views for reporting / BI
        ├── dim_meetings.sql
        ├── dim_sessions.sql
        ├── dim_drivers.sql
        ├── fct_weather.sql
        ├── fct_driver_sessions.sql
        ├── rpt_weather_by_session.sql
        └── rpt_race_results.sql
```

| Model | Schema | Purpose |
|---|---|---|
| `stg_meetings` | `staging` | Race weekend metadata with proper types (`meeting_key`, `date_start`, `year`) |
| `stg_sessions` | `staging` | Session metadata with typed dates, typed against `meeting_key` |
| `stg_drivers` | `staging` | Per-session driver registrations (identity, team, broadcast names) |
| `stg_weather` | `staging` | Typed weather readings plus an `is_raining` flag |
| `stg_positions` | `staging` | Typed running-position readings per driver per lap |
| `dim_meetings` | `drivers_gold` | Deduplicated dimension, one row per race weekend |
| `dim_sessions` | `drivers_gold` | One row per session, enriched with meeting info |
| `dim_drivers` | `drivers_gold` | Deduplicated driver dimension, one row per `driver_number` |
| `fct_weather` | `drivers_gold` | Fact table, one row per weather observation |
| `fct_driver_sessions` | `drivers_gold` | Driver appearances per session with team + meeting context |
| `rpt_weather_by_session` | `drivers_gold` | Per-session weather summary (averages, min/max temps, rainfall share) |
| `rpt_race_results` | `drivers_gold` | Final position per driver per session — `final_position = 1` is the winner |

All models are materialized as **views**, so the marts always reflect the latest raw data. The `drivers_gold` schema is the analytics layer — ready to be queried in SQL or consumed by any BI tool. Examples:

```sql
-- Who won each race?
select s.circuit_short_name, d.full_name
from drivers_gold.rpt_race_results r
join drivers_gold.dim_sessions s using (session_key)
join drivers_gold.dim_drivers d using (driver_number)
where r.final_position = 1
  and s.session_type = 'Race'
order by r.session_key;
```

`rpt_race_results` covers all session types (Practice, Qualifying, Sprint, Race) — filter `session_type = 'Race'` (or `'Sprint'`) for results of that session kind.

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

## Data dictionary

### Naming conventions

dbt model names use prefixes that tell you what a model is:

| Prefix | Meaning | Layer |
|---|---|---|
| `stg_` | **staging** — cleaned, typed, filtered view directly over a `raw` table | `staging` |
| `dim_` | **dimension** — reference/descriptive data, one row per entity (meeting, session, driver) | `drivers_gold` |
| `fct_` | **fact** — measurable events/observations (weather readings, driver appearances) | `drivers_gold` |
| `rpt_` | **report** — pre-aggregated analytical view for direct consumption | `drivers_gold` |

### Entities

- **Meeting** — a race weekend (e.g. "Dutch Grand Prix"). Identified by `meeting_key`; carries the circuit, country, dates, and year.
- **Session** — a single track activity inside a meeting: `Practice`, `Qualifying`, `Sprint`, `Race`. Identified by `session_key` and described by `session_type`/`session_name`.
- **Driver** — a competitor. Identified by `driver_number` (race number); described by `full_name`, `name_acronym`, and `country_code`. Teams are per-session (`team_name`, `team_colour` in `fct_driver_sessions`).
- **Weather reading** — an observation (`recorded_at`) of a session's conditions: air/track temperature (°C), humidity (%), pressure (hPa), wind speed (km/h) and direction (°), rainfall (0/1 → `is_raining`).
- **Position** — a driver's running position at a point in time; the final reading per driver in a session is the result (`final_position`, where `1` = winner).

### Raw tables (`raw` schema)

| Table | What each row is |
|---|---|
| `meetings_raw` | One row per race weekend |
| `sessions_raw` | One row per session (practice/quali/sprint/race) |
| `drivers_raw` | One row per driver registration in a session |
| `weather_raw` | One row per weather observation |
| `positions_raw` | One row per running-position reading per driver |

### Gold views (`drivers_gold` schema)

| View | Granularity | Use for |
|---|---|---|
| `dim_meetings` | 1 row / meeting | Meeting attributes, deduped |
| `dim_sessions` | 1 row / session | Session attributes + meeting name/circuit context |
| `dim_drivers` | 1 row / driver | Driver identity (name, acronym, nationality) |
| `fct_weather` | 1 row / weather observation | Raw conditions per timestamp |
| `fct_driver_sessions` | 1 row / driver in a session | Who raced where, and for which team |
| `rpt_weather_by_session` | 1 row / session | Session-level weather summary (averages, min/max, rain share) |
| `rpt_race_results` | 1 row / driver per session | Final positions; race winners at `final_position = 1` |

### Key fields

| Field | Meaning |
|---|---|
| `meeting_key` | Unique id of a race weekend |
| `session_key` | Unique id of a session |
| `driver_number` | Driver's race number (stable id) |
| `final_position` | Position a driver finished a session in |
| `is_raining` | True when a weather reading has `rainfall > 0` |
| `recorded_at` | Timestamp of the observation/reading |
| `ingestion_time` | When Airflow loaded the row |

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

**`AIRFLOW_HOME` must point at this repo's `airflow/` dir** — without it Airflow silently falls back to `~/airflow/` (different DB, different dags, different login password). First start: log in as `admin` with the password in `airflow/simple_auth_manager_passwords.json.generated`, then register the Postgres connection and unpause the DAGs (DAGs are paused at creation by default):

```sh
export AIRFLOW_HOME="$PWD/airflow"
airflow connections add conn_postgres \
  --conn-type postgres --conn-host localhost --conn-port 5000 \
  --conn-login postgres --conn-password postgres --conn-schema dw
```

See `AGENTS.md` for full details on configuration and verification.

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
