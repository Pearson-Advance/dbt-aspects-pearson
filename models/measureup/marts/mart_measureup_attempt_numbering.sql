{{
    config(
        tags=[
            "measureup",
            "measureup_marts",
            "measureup_attempts",
        ],
    )
}}

with
    attempts as (
        select
            context.exam_attempt_id,
            context.exam_test_id,
            context.lti_user_id,
            context.lti_course_id,
            lower(trim(coalesce(context.mode, ''))) as attempt_mode,
            parseDateTime64BestEffortOrNull(
                context.attempt_initiated_datetime, 6, 'UTC'
            ) as started_at,
            context.has_lti_context
            and context.has_licensed_class_context
            and coalesce(context.lti_user_id, '') != ''
            and coalesce(context.lti_course_id, '') != ''
            and coalesce(context.exam_test_id, '') != ''
            and attempt_mode != ''
            and context.flow_state = 'finished'
            and started_at is not null as can_number
        from {{ ref("mart_measureup_attempt_context") }} as context
    )

select
    exam_attempt_id,
    if(
        can_number,
        row_number() over (
            -- Ineligible starts must not shift the qualifying sequence.
            partition by
                lti_user_id, lti_course_id, exam_test_id, attempt_mode, can_number
            order by started_at, toUInt64OrZero(exam_attempt_id)
        ),
        null
    ) as attempt_number
from attempts
