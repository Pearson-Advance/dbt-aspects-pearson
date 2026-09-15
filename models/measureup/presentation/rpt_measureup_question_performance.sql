{{
    config(
        tags=[
            "measureup",
            "measureup_presentation",
        ],
    )
}}

{#
    Dashboard-ready grain: one row per current question-attempt result.
    Three hops back to the attempt/context (question_attempt ->
    attempt_subdomain -> attempt_domain -> mart), each FK-to-source_id, at
    most 1:1.

    question.content (the restricted question text/choices JSON) and
    question_attempt.answer (the restricted learner-answer JSON) are
    deliberately not projected here — dashboard-ready views must not
    expose them.
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

    attempt_domain.exam_attempt_id as exam_attempt_id,
    attempt_domain.exam_domain_id as exam_domain_id,

    mart.exam_test_id as exam_test_id,
    mart.exam_test_name as exam_test_name,
    mart.flow_state as flow_state,
    mart.is_passed as is_passed,
    mart.has_lti_context as has_lti_context,
    mart.has_licensed_class_context as has_licensed_class_context,
    mart.institution_ccx_id as institution_ccx_id,
    mart.institution_id as institution_id,
    mart.license_id as license_id

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
    {{ ref("mart_measureup_attempt_context") }} as mart
    on attempt_domain.exam_attempt_id = mart.exam_attempt_id
