{{
    config(
        tags=[
            "measureup",
            "measureup_presentation",
        ],
    )
}}

{#
    Dashboard-ready grain: one row per current readiness record, at
    enrollment grain. Deliberately independent of exam attempts —
    readiness is a property of the learner's course enrollment as of now,
    not of any specific attempt, so it must not be joined to
    mart_measureup_attempt_context or duplicated across an enrollment's
    historical attempts.
#}
select
    source_id as exam_ready_status_id,
    course_enrollment_id,
    user_id,
    course_id,
    enrollment_mode,
    enrollment_is_active,
    status,
    last_score,
    status_updated_at
from {{ ref("int_measureup_exam_ready_status_current") }}
