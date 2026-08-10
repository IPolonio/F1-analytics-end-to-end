with source as (
    select * from {{ source('raw', 'weather_raw') }}
),

renamed as (
    select
        session_key::bigint as session_key,
        date::timestamp as recorded_at,
        air_temperature::numeric as air_temperature,
        track_temperature::numeric as track_temperature,
        humidity::numeric as humidity,
        pressure::numeric as pressure,
        rainfall::numeric as rainfall,
        wind_direction::numeric as wind_direction,
        wind_speed::numeric as wind_speed,
        rainfall > 0 as is_raining,
        source,
        ingestion_time
    from source
    where session_key is not null
      and date is not null
)

select * from renamed
