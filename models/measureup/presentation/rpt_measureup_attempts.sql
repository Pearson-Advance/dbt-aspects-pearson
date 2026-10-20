{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(institution_id, lti_course_id, attempt_initiated_datetime, exam_attempt_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
        },
        tags=[
            "measureup",
            "measureup_presentation",
        ],
    )
}}

{#
    Dashboard-ready grain: one row per current student exam attempt, built
    on mart_measureup_attempt_context (test version, LTI context, class and
    attempt_user_type are resolved there). Only 'student' attempts are
    kept: the LTI unit creates an attempt for any role that opens it.

    Learner identity is joined here, not in the mart, so other views do not
    inherit personal data. The enrollment lookup is grouped per (user,
    class), preferring a row with a profile name, then the highest
    source_id; student_name falls back to the username when there is no
    name.

    total counts every question result, including unscored ones, because
    MeasureUp's final_score counts those as incorrect.

    Refreshable materialized view (every 4 hours, atomic replace), ordered
    by institution, class and start time for dashboard filters and
    row-level security. Changing columns or ordering needs a
    --full-refresh. attempt_*_at are the text timestamps parsed once per
    refresh. mup_attempt_configuration is never projected.
#}
select
    mart.exam_attempt_id as exam_attempt_id,
    mart.exam_test_id as exam_test_id,
    mart.exam_test_name as exam_test_name,
    mart.exam_test_product_id as exam_test_product_id,
    mart.flow_state as flow_state,
    mart.attempt_initiated_datetime as attempt_initiated_datetime,
    mart.attempt_finished_datetime as attempt_finished_datetime,
    parseDateTime64BestEffortOrNull(
        mart.attempt_initiated_datetime, 6, 'UTC'
    ) as attempt_initiated_at,
    parseDateTime64BestEffortOrNull(
        mart.attempt_finished_datetime, 6, 'UTC'
    ) as attempt_finished_at,
    mart.attempt_duration as attempt_duration,
    mart.final_score as final_score,
    mart.is_passed as is_passed,
    mart.mode as mode,
    mart.has_lti_context as has_lti_context,
    mart.lti_user_id as lti_user_id,
    mart.lti_course_id as lti_course_id,
    mart.has_licensed_class_context as has_licensed_class_context,
    mart.institution_ccx_id as institution_ccx_id,
    mart.institution_id as institution_id,
    mart.license_id as license_id,
    mart.class_name as class_name,

    if(
        coalesce(enrollment.student_name, '') != '',
        enrollment.student_name,
        enrollment.student
    ) as student_name,
    enrollment.student as student_username,

    coalesce(question_summary.correct, 0) as correct,
    coalesce(question_summary.total, 0) as total

from {{ ref("mart_measureup_attempt_context") }} as mart
left join
    (
        select
            src.user_id as user_id,
            src.class_id as class_id,
            argMax(src.student, (src.student_name != '', src.source_id)) as student,
            argMax(
                src.student_name, (src.student_name != '', src.source_id)
            ) as student_name
        from {{ ref("int_course_licensing_licensed_enrollment_current") }} as src
        group by src.user_id, src.class_id
    ) as enrollment
    on mart.lti_user_id = enrollment.user_id
    and mart.lti_course_id = enrollment.class_id
left join
    (
        select
            attempt_domain.exam_attempt_id as exam_attempt_id,
            countIf(toFloat64OrNull(question_attempt.score) = 1) as correct,
            count() as total
        from
            {{ ref("int_measureup_exam_question_attempt_current") }} as question_attempt
        left join
            {{ ref("int_measureup_exam_attempt_subdomain_current") }}
            as attempt_subdomain
            on question_attempt.subdomain_attempt_id = attempt_subdomain.source_id
        left join
            {{ ref("int_measureup_exam_attempt_domain_current") }} as attempt_domain
            on attempt_subdomain.attempt_domain_id = attempt_domain.source_id
        group by attempt_domain.exam_attempt_id
    ) as question_summary
    on mart.exam_attempt_id = question_summary.exam_attempt_id
where mart.attempt_user_type = 'student'
