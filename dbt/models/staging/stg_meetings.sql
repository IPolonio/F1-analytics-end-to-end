with source as (
    select * from {{ source('raw', 'meetings_raw') }}
),

renamed as (
    select
        meeting_key::bigint as meeting_key,
        circuit_short_name,
        country_code,
        country_name,
        date_start::timestamp as date_start,
        gmt_offset,
        location,
        meeting_name,
        meeting_official_name,
        year::integer as year,
        source,
        ingestion_time
    from source
    where meeting_key is not null
)

select * from renamed
