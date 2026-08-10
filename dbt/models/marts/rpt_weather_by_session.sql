with weather as (
    select * from {{ ref('stg_weather') }}
),

sessions as (
    select
        session_key,
        meeting_key,
        session_name,
        session_type,
        circuit_short_name,
        year
    from {{ ref('dim_sessions') }}
)

select
    w.session_key,
    w.meeting_key,
    s.session_name,
    s.session_type,
    s.circuit_short_name,
    s.year,
    count(*) as sample_count,
    min(w.recorded_at) as first_recorded_at,
    max(w.recorded_at) as last_recorded_at,
    avg(w.air_temperature) as avg_air_temperature,
    min(w.air_temperature) as min_air_temperature,
    max(w.air_temperature) as max_air_temperature,
    avg(w.track_temperature) as avg_track_temperature,
    min(w.track_temperature) as min_track_temperature,
    max(w.track_temperature) as max_track_temperature,
    avg(w.humidity) as avg_humidity,
    avg(w.pressure) as avg_pressure,
    avg(w.wind_speed) as avg_wind_speed,
    avg(w.wind_direction) as avg_wind_direction,
    sum(case when w.is_raining then 1 else 0 end) as rainy_sample_count,
    avg(w.rainfall) as rainfall_share
from weather w
left join sessions s on w.session_key = s.session_key
group by
    w.session_key,
    w.meeting_key,
    s.session_name,
    s.session_type,
    s.circuit_short_name,
    s.year
order by w.session_key
