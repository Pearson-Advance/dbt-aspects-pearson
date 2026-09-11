{{
    config(
        tags=[
            "measureup",
            "measureup_presentation",
        ],
    )
}}

{#
    Dashboard-ready grain: one row per current attempt-domain result.
    Both joins are FK-to-source_id, at most 1:1 — cannot multiply
    attempt_domain rows. Row-count-equals-base is verified independently
    against real ClickHouse, not just asserted by this join shape.
#}
select
    attempt_domain.source_id as attempt_domain_id,
    attempt_domain.exam_attempt_id as exam_attempt_id,
    attempt_domain.exam_domain_id as exam_domain_id,
    exam_domain.name as exam_domain_name,
    attempt_domain.average_score as average_score,

    mart.exam_test_id as exam_test_id,
    mart.exam_test_name as exam_test_name,
    mart.flow_state as flow_state,
    mart.is_passed as is_passed,
    mart.has_lti_context as has_lti_context,
    mart.has_licensed_class_context as has_licensed_class_context,
    mart.institution_ccx_id as institution_ccx_id,
    mart.institution_id as institution_id,
    mart.license_id as license_id

from {{ ref("int_measureup_exam_attempt_domain_current") }} as attempt_domain
left join
    {{ ref("int_measureup_exam_domain_current") }} as exam_domain
    on attempt_domain.exam_domain_id = exam_domain.source_id
left join
    {{ ref("mart_measureup_attempt_context") }} as mart
    on attempt_domain.exam_attempt_id = mart.exam_attempt_id
