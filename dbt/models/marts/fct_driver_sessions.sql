with drivers as (
    select * from {{ ref('stg_drivers') }}
),

sessions as (
    select
        session_key,
        meeting_key,
        session_name,
        session_type,
        date_start,
        year
    from {{ ref('dim_sessions') }}
)

select
    d.session_key,
    d.meeting_key,
    d.driver_number,
    d.full_name,
    d.name_acronym,
    d.team_name,
    d.team_colour,
    d.country_code,
    s.session_name,
    s.session_type,
    s.date_start,
    s.year
from drivers d
left join sessions s on d.session_key = s.session_key
