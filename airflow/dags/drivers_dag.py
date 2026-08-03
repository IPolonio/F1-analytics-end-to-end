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
        def load_to_postgres():
            import json
            from urllib.request import urlopen
            import pandas as pd
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            # 1. Extract
            with urlopen(api_url) as response:
                data = json.loads(response.read().decode('utf-8'))

            df = pd.DataFrame(data)

            # 2. Add metadata columns
            df['source'] = source_name
            df['ingestion_time'] = datetime.utcnow()

            print(f"Data Preview for {table_name}:")
            print(df.head())

            # 3. Load
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

        load_to_postgres()

    return ingestion_pipeline()


# --- Instantiate DAGs from the factory ---

f1_location_dag = create_ingestion_dag(
    dag_id='f1_meetings_ingestion',
    api_url='https://api.openf1.org/v1/meetings?year=2025',
    table_name='meetings_raw',
    source_name='openf1_api',
    tags=['f1', 'postgres'],
)

