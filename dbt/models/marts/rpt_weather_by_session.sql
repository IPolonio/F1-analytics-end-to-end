with weather as (
    select * from {{ ref('stg_weather') }}
)

select
    session_key,
    count(*) as sample_count,
    min(recorded_at) as first_recorded_at,
    max(recorded_at) as last_recorded_at,
    avg(air_temperature) as avg_air_temperature,
    min(air_temperature) as min_air_temperature,
    max(air_temperature) as max_air_temperature,
    avg(track_temperature) as avg_track_temperature,
    min(track_temperature) as min_track_temperature,
    max(track_temperature) as max_track_temperature,
    avg(humidity) as avg_humidity,
    avg(pressure) as avg_pressure,
    avg(wind_speed) as avg_wind_speed,
    avg(wind_direction) as avg_wind_direction,
    sum(case when is_raining then 1 else 0 end) as rainy_sample_count,
    avg(rainfall) as rainfall_share
from weather
group by session_key
order by session_key
