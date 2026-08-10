with positions as (
    select * from {{ ref('stg_positions') }}
),

sessions as (
    select
        session_key,
        meeting_key,
        session_name,
        session_type,
        circuit_short_name,
        date_start,
        year
    from {{ ref('dim_sessions') }}
),

-- last position reading per driver within each session = final race position
final_positions as (
    select distinct on (session_key, driver_number)
        session_key,
        meeting_key,
        driver_number,
        position as final_position,
        recorded_at
    from positions
    order by session_key, driver_number, recorded_at desc
)

select
    fp.session_key,
    fp.meeting_key,
    fp.driver_number,
    fp.final_position,
    s.session_name,
    s.session_type,
    s.circuit_short_name,
    s.year,
    s.date_start
from final_positions fp
left join sessions s on fp.session_key = s.session_key
order by fp.session_key, fp.final_position
