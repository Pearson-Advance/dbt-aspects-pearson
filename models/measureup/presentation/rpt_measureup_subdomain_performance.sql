{{
    config(
        tags=[
            "measureup",
            "measureup_presentation",
        ],
    )
}}

{#
    Dashboard-ready grain: one row per current attempt-subdomain result.
    attempt_subdomain only carries attempt_domain_id, not exam_attempt_id
    directly — attempt_domain is the hop back to the attempt/context, not
    just a name lookup here. Every join is FK-to-source_id, at most 1:1.
#}
select
    attempt_subdomain.source_id as attempt_subdomain_id,
    attempt_subdomain.attempt_domain_id as attempt_domain_id,
    attempt_subdomain.exam_subdomain_id as exam_subdomain_id,
    exam_subdomain.name as exam_subdomain_name,
    attempt_subdomain.average_score as average_score,

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

from {{ ref("int_measureup_exam_attempt_subdomain_current") }} as attempt_subdomain
left join
    {{ ref("int_measureup_exam_subdomain_current") }} as exam_subdomain
    on attempt_subdomain.exam_subdomain_id = exam_subdomain.source_id
left join
    {{ ref("int_measureup_exam_attempt_domain_current") }} as attempt_domain
    on attempt_subdomain.attempt_domain_id = attempt_domain.source_id
left join
    {{ ref("mart_measureup_attempt_context") }} as mart
    on attempt_domain.exam_attempt_id = mart.exam_attempt_id
