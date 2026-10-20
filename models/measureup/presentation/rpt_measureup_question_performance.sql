{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(institution_id, lti_course_id, attempt_initiated_datetime, exam_attempt_id, question_attempt_id)",
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
    Dashboard-ready grain: one row per current question-attempt result.
    Every join is FK-to-source_id, at most 1:1: three hops back to the
    attempt (question_attempt -> attempt_subdomain -> attempt_domain ->
    mart) plus the domain and subdomain catalogs. Catalog names and the
    attempt's mode, dates and class are resolved here so dashboard
    datasets read this model without joining anything.

    Only results of 'student' attempts are kept, as in
    rpt_measureup_attempts, so instructor launches do not count toward
    domain scores. A question whose chain does not reach an attempt cannot
    be classified and is dropped too.

    Refreshable materialized view (every 4 hours, atomic replace), ordered
    by institution, class and start time for dashboard filters and
    row-level security. Changing columns or ordering needs a
    --full-refresh. attempt_*_at are the text timestamps parsed once per
    refresh. question.content and question_attempt.answer (restricted
    JSON) are never projected.
#}
select
    question_attempt.source_id as question_attempt_id,
    question_attempt.subdomain_attempt_id as subdomain_attempt_id,
    question_attempt.question_id as question_id,
    question.name as question_name,
    question.question_type as question_type,
    question_attempt.score as score,
    question_attempt.answer_shown as answer_shown,
    question_attempt.review_marked as review_marked,
    question_attempt.status as question_attempt_status,
    question_attempt.displayed as displayed,

    attempt_subdomain.attempt_domain_id as attempt_domain_id,
    attempt_subdomain.exam_subdomain_id as exam_subdomain_id,
    exam_subdomain.name as exam_subdomain_name,

    attempt_domain.exam_attempt_id as exam_attempt_id,
    attempt_domain.exam_domain_id as exam_domain_id,
    exam_domain.name as exam_domain_name,

    mart.exam_test_id as exam_test_id,
    mart.exam_test_name as exam_test_name,
    mart.flow_state as flow_state,
    mart.is_passed as is_passed,
    mart.mode as mode,
    mart.attempt_initiated_datetime as attempt_initiated_datetime,
    mart.attempt_finished_datetime as attempt_finished_datetime,
    parseDateTime64BestEffortOrNull(
        mart.attempt_initiated_datetime, 6, 'UTC'
    ) as attempt_initiated_at,
    parseDateTime64BestEffortOrNull(
        mart.attempt_finished_datetime, 6, 'UTC'
    ) as attempt_finished_at,
    mart.has_lti_context as has_lti_context,
    mart.lti_course_id as lti_course_id,
    mart.has_licensed_class_context as has_licensed_class_context,
    mart.institution_ccx_id as institution_ccx_id,
    mart.institution_id as institution_id,
    mart.license_id as license_id,
    mart.class_name as class_name

from {{ ref("int_measureup_exam_question_attempt_current") }} as question_attempt
left join
    {{ ref("int_measureup_question_current") }} as question
    on question_attempt.question_id = question.source_id
left join
    {{ ref("int_measureup_exam_attempt_subdomain_current") }} as attempt_subdomain
    on question_attempt.subdomain_attempt_id = attempt_subdomain.source_id
left join
    {{ ref("int_measureup_exam_attempt_domain_current") }} as attempt_domain
    on attempt_subdomain.attempt_domain_id = attempt_domain.source_id
left join
    {{ ref("int_measureup_exam_subdomain_current") }} as exam_subdomain
    on attempt_subdomain.exam_subdomain_id = exam_subdomain.source_id
left join
    {{ ref("int_measureup_exam_domain_current") }} as exam_domain
    on attempt_domain.exam_domain_id = exam_domain.source_id
left join
    {{ ref("mart_measureup_attempt_context") }} as mart
    on attempt_domain.exam_attempt_id = mart.exam_attempt_id
where mart.attempt_user_type = 'student'
