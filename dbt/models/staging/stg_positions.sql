with source as (
    select * from {{ source('raw', 'positions_raw') }}
),

renamed as (
    select
        session_key::bigint as session_key,
        meeting_key::bigint as meeting_key,
        driver_number::bigint as driver_number,
        position::integer as position,
        date::timestamp as recorded_at,
        source,
        ingestion_time
    from source
    where session_key is not null
      and driver_number is not null
      and position is not null
)

select * from renamed
