from datetime import datetime, timedelta
from airflow.sdk import dag, task


def create_ingestion_dag(
    dag_id: str,
    api_url: str,
    table_name: str,
    source_name: str,
    schema: str = 'raw',
    postgres_conn_id: str = 'conn_postgres',
    schedule=None,
    tags=None,
):
    """
    Factory that builds a generic API -> Postgres ingestion DAG.

    Args:
        dag_id: Unique DAG identifier.
        api_url: Endpoint to pull JSON data from.
        table_name: Destination table name in Postgres.
        source_name: Value stamped in the 'source' column.
        schema: Destination schema (default 'raw').
        postgres_conn_id: Airflow connection id for Postgres.
        schedule: Cron expression or None for manual trigger.
        tags: List of tags for UI filtering.
    """

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

    @dag(
        dag_id=dag_id,
        default_args=default_args,
        description=f'Fetch {source_name} data and load into {schema}.{table_name}',
        schedule=schedule,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=tags or [],
    )
    def ingestion_pipeline():

        @task()
        def load_positions_2025():
            import json
            import time
            from urllib.request import urlopen, Request
            from urllib.error import HTTPError
            import pandas as pd
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            def fetch_with_retry(url, max_retries=5, base_delay=2):
                """Fetch JSON with exponential backoff retries on HTTP 429."""
                for attempt in range(max_retries):
                    try:
                        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                        with urlopen(req) as response:
                            return json.loads(response.read().decode('utf-8'))
                    except HTTPError as e:
                        if e.code == 429:
                            wait = base_delay * (2 ** attempt)
                            print(f"429 received, waiting {wait}s before retrying (attempt {attempt + 1}/{max_retries})")
                            time.sleep(wait)
                        else:
                            raise
                raise Exception(f"Retries exhausted for {url}")

            # 1. Get all 2025 sessions
            sessions_url = 'https://api.openf1.org/v1/sessions?year=2025'
            sessions = fetch_with_retry(sessions_url)
            session_keys = [s['session_key'] for s in sessions]
            print(f"Found {len(session_keys)} sessions in 2025")

            # 2. Per session, fetch its position data (running position per driver per lap)
            all_positions = []
            for i, session_key in enumerate(session_keys):
                position_url = f'https://api.openf1.org/v1/position?session_key={session_key}'
                data = fetch_with_retry(position_url)
                all_positions.extend(data)
                print(f"Session {session_key}: {len(data)} position records")

                # Preventative pause between requests to avoid the rate limit
                time.sleep(0.5)

            df = pd.DataFrame(all_positions)
            df['source'] = source_name
            df['ingestion_time'] = pd.Timestamp.utcnow()

            print("Data Preview:")
            print(df.head())

            # 3. Load to Postgres
            hook = PostgresHook(postgres_conn_id=postgres_conn_id)
            engine = hook.get_sqlalchemy_engine()

            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',
                schema=schema,
                index=False
            )
            print(f"Data successfully loaded to {schema}.{table_name}!")

        load_positions_2025()

    return ingestion_pipeline()


# --- Instantiate the DAG from the factory ---

positions_dag = create_ingestion_dag(
    dag_id='f1_positions_ingestion',
    api_url='https://api.openf1.org/v1/position?',  # per-session fetch happens inside the task
    table_name='positions_raw',
    source_name='openf1_api',
    tags=['f1', 'postgres', 'positions'],
)
