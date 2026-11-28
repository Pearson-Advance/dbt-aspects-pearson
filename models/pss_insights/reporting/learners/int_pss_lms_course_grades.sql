{{ config(tags=["pss_reporting"]) }}

-- The LMS CourseGradedTransformer emits one earned statement with the course
-- as its object. Preserve field presence before the Aspects grading projection
-- converts an absent scaled score to zero. A replay keeps its original clock.
with
    events as (
        select
            course_key as ccx_id,
            actor_id,
            event_id,
            parseDateTime64BestEffortOrNull(
                JSONExtractString(event, 'timestamp'), 6, 'UTC'
            ) as graded_at,
            toDateTime64(emission_time, 6, 'UTC') as received_at,
            JSONExtractRaw(event, 'result', 'score', 'scaled') as scaled,
            toFloat64OrNull(scaled) as fraction
        from {{ ref("xapi_events_all_parsed") }}
        where
            verb_id = 'http://id.tincanapi.com/verb/earned'
            and endsWith(object_id, concat('/course/', course_key))
            and JSONExtractString(event, 'object', 'definition', 'type')
            = 'http://adlnet.gov/expapi/activities/course'
            and course_key in (select ccx_id from {{ ref("int_pss_class_scope") }})
    ),
    statements as (
        select
            ccx_id,
            actor_id,
            event_id,
            graded_at,
            min(received_at) as first_received,
            any(tuple(e.fraction)) .1 as fraction,
            uniqExact(tuple(e.fraction)) = 1 as consistent,
            count() over (partition by event_id) as identities
        from events as e
        group by ccx_id, actor_id, event_id, graded_at
    ),
    updates as (
        select
            ccx_id,
            actor_id,
            coalesce(graded_at, first_received) as update_time,
            if(
                countIf(
                    identities != 1
                    or not consistent
                    or graded_at is null
                    or isNull(fraction)
                    or not (fraction between 0 and 1)
                )
                = 0
                and uniqExact(tuple(fraction)) = 1,
                any(tuple(fraction)) .1,
                null
            ) as grade
        from statements
        group by ccx_id, actor_id, update_time
    )
select ccx_id, actor_id, argMax(tuple(grade), update_time) .1 as course_grade
from updates
group by ccx_id, actor_id
