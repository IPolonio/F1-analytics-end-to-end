with source as (
    select * from {{ source('raw', 'sessions_raw') }}
),

renamed as (
    select
        session_key,
        meeting_key,
        session_type,
        session_name,
        date_start::timestamp as date_start,
        date_end::timestamp as date_end,
        circuit_key,
        circuit_short_name,
        country_key,
        country_code,
        country_name,
        location,
        gmt_offset,
        year::integer as year,
        is_cancelled,
        source,
        ingestion_time
    from source
    where session_key is not null
)

select * from renamed
