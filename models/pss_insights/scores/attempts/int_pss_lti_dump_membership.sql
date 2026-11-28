{{ config(tags=["pss_scores"]) }}

-- Read all rows for an observed master dump, including a dump with zero LTI
-- blocks. A prior positive observation cannot fill a later collection gap.
with
    blocks as (
        select
            course_key,
            dump_id,
            location,
            parseDateTime64BestEffortOrNull(time_last_dumped, 6, 'UTC') as observed_at
        from {{ source("event_sink", "course_blocks") }}
        where
            course_key
            in (select master_course from {{ ref("int_pss_master_course_scope") }})
    ),
    clocks as (
        select
            *,
            uniqExact(tuple(observed_at)) over (
                partition by course_key, dump_id
            ) as dump_clock_count,
            countIf(observed_at is null) over (partition by course_key) as undated_rows
        from blocks
    )
select
    course_key as master_course,
    assumeNotNull(observed_at) as observed_at_utc,
    uniqExact(dump_id) = 1
    and max(dump_clock_count) = 1
    and max(undated_rows) = 0 as dump_order_known,
    groupUniqArrayIf(
        location,
        position(location, '+type@lti_consumer+block@') > 0
        or position(location, '+type@lti+block@') > 0
    ) as lti_blocks
from clocks
where observed_at is not null
group by course_key, observed_at
