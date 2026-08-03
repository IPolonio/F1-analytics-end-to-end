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
        def load_weather_2025():
            import json
            import time
            from urllib.request import urlopen, Request
            from urllib.error import HTTPError
            import pandas as pd
            from airflow.providers.postgres.hooks.postgres import PostgresHook

            def fetch_with_retry(url, max_retries=5, base_delay=2):
                """Hace la petición con reintentos y backoff exponencial si hay 429."""
                for attempt in range(max_retries):
                    try:
                        req = Request(url, headers={'User-Agent': 'Mozilla/5.0'})
                        with urlopen(req) as response:
                            return json.loads(response.read().decode('utf-8'))
                    except HTTPError as e:
                        if e.code == 429:
                            wait = base_delay * (2 ** attempt)
                            print(f"429 recibido, esperando {wait}s antes de reintentar (intento {attempt + 1}/{max_retries})")
                            time.sleep(wait)
                        else:
                            raise
                raise Exception(f"Se agotaron los reintentos para {url}")

            # 1. Obtener todas las sesiones de 2025
            sessions_url = 'https://api.openf1.org/v1/sessions?year=2025'
            sessions = fetch_with_retry(sessions_url)

            session_keys = [s['session_key'] for s in sessions]
            print(f"Encontradas {len(session_keys)} sesiones en 2025")

            # 2. Por cada sesión, traer su weather data (con pausa entre requests)
            all_weather_data = []
            for i, session_key in enumerate(session_keys):
                weather_url = f'https://api.openf1.org/v1/weather?session_key={session_key}'
                data = fetch_with_retry(weather_url)
                all_weather_data.extend(data)
                print(f"Sesión {session_key}: {len(data)} registros de clima")

                # Pausa preventiva entre requests para no gatillar el rate limit
                time.sleep(0.5)

            df = pd.DataFrame(all_weather_data)
            df['source'] = source_name
            df['ingestion_time'] = pd.Timestamp.utcnow()

            print("Data Preview:")
            print(df.head())

            # 3. Cargar a Postgres
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

        load_weather_2025()

    return ingestion_pipeline()


# --- Instanciar el DAG desde la fábrica ---

weather_dag = create_ingestion_dag(
    dag_id='f1_weather_ingestion',
    api_url='https://api.openf1.org/v1/weather?',  # no se usa directamente en este task, pero se mantiene por consistencia con la firma de la fábrica
    table_name='weather_raw',
    source_name='openf1_api',
    tags=['f1', 'postgres', 'weather'],
)