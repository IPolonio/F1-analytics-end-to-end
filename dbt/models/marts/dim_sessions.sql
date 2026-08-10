with sessions as (
    select * from {{ ref('stg_sessions') }}
),

meetings as (
    select * from {{ ref('stg_meetings') }}
)

select
    s.session_key,
    s.meeting_key,
    s.session_type,
    s.session_name,
    s.date_start,
    s.date_end,
    s.circuit_key,
    s.circuit_short_name,
    s.country_code,
    s.country_name,
    s.location,
    s.gmt_offset,
    s.year,
    s.is_cancelled,
    m.meeting_name,
    m.meeting_official_name
from sessions s
left join meetings m on s.meeting_key = m.meeting_key
