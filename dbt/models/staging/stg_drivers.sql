with source as (
    select * from {{ source('raw', 'drivers_raw') }}
),

renamed as (
    select
        meeting_key,
        session_key,
        driver_number,
        broadcast_name,
        full_name,
        name_acronym,
        team_name,
        team_colour,
        first_name,
        last_name,
        headshot_url,
        country_code,
        source,
        ingestion_time
    from source
    where session_key is not null
      and driver_number is not null
)

select * from renamed
