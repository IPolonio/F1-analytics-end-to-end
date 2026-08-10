with source as (
    select * from {{ ref('stg_weather') }}
)

select
    session_key,
    meeting_key,
    recorded_at,
    air_temperature,
    track_temperature,
    humidity,
    pressure,
    rainfall,
    is_raining,
    wind_speed,
    wind_direction,
    ingestion_time
from source
