with source as (
    select * from {{ ref('stg_drivers') }}
),

deduplicated as (
    select distinct on (driver_number) *
    from source
    order by driver_number, ingestion_time desc
)

select
    driver_number,
    full_name,
    first_name,
    last_name,
    name_acronym,
    broadcast_name,
    country_code,
    headshot_url
from deduplicated
order by driver_number
