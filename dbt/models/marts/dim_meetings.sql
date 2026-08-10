with source as (
    select * from {{ ref('stg_meetings') }}
),

deduplicated as (
    select distinct on (meeting_key) *
    from source
    order by meeting_key, date_start desc
)

select
    meeting_key,
    meeting_name,
    meeting_official_name,
    circuit_short_name,
    location,
    country_name,
    country_code,
    year,
    date_start,
    gmt_offset
from deduplicated
order by date_start
