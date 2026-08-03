# AGENTS.md

Apache Airflow 3.3 (LocalExecutor, SQLite metastore) project ingesting OpenF1 API data into Postgres. Python 3.14, managed with `uv`.

## Setup / running Airflow

- Install deps: `uv sync`. Virtualenv is `.venv/`.
- Airflow config is committed at `airflow/airflow.cfg` and uses **absolute paths** into this repo's `airflow/` dir (dags, sqlite db, logs, plugins). To actually use it, every `airflow` command needs `AIRFLOW_HOME` pointed at it:
  ```sh
  export AIRFLOW_HOME="$PWD/airflow"
  ```
  Without it, Airflow silently falls back to a generated config at `~/airflow/` (different dags folder, different sqlite db).
- Components: `airflow webserver`, `airflow scheduler`, or all-in-one `airflow standalone`. UI login user is `admin` (SimpleAuthManager; password auto-generated into `airflow/simple_auth_manager_passwords.json.generated`).

## Verifying DAGs

- `airflow dags list-import-errors` — parse failures show up here (run after adding/editing a DAG).
- `airflow dags test <dag_id> <logical_date>` — run one DAG's tasks in-process against the DB.

## DAG conventions

- Use the Airflow 3.x API only: `from airflow.sdk import dag, task`, `schedule=` argument. The older `airflow.decorators` / `schedule_interval=` style will break.
- DAGs live in `airflow/dags/`. Both current DAGs (`f1_meetings_ingestion`, `f1_weather_ingestion`) are built from a `create_ingestion_dag` factory that is **copy-pasted in each file** (`drivers_dag.py`, `load_weather.py`) — there is no shared module. Changes to the factory must be applied in both files.
- Tasks need an Airflow Postgres connection with id `conn_postgres` and a target schema `raw` to exist, or they fail at runtime.
- `dags_are_paused_at_creation = True`: new DAGs won't schedule until unpaused (UI or `airflow dags unpause`).
- `load_examples = True`: Airflow's example DAGs are loaded alongside the project's (visible in UI/logs).
- OpenF1 rate-limits (HTTP 429); the weather DAG retries with exponential backoff and sleeps 0.5s between requests. Keep that behavior when editing.

## Git gotchas

- Repo has **no commits yet** and **no `.gitignore`**. Never stage:
  - `.venv/`
  - `airflow/airflow.db*` (SQLite metastore) and `airflow/logs/`
  - `airflow/simple_auth_manager_passwords.json.generated` (contains a plaintext admin password)
  - `__pycache__/`
