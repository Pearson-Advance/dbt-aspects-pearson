{{
    config(
        tags=[
            "measureup",
            "measureup_presentation",
        ],
    )
}}

{#
    Dashboard-ready grain: one row per current exam attempt. Built directly
    on mart_measureup_attempt_context, which already resolves the test
    version, LTI context, and licensed-class ownership — nothing here
    re-derives that join chain.

    mup_attempt_configuration (restricted JSON) is not projected — dropped
    already at the mart layer.
#}
select
    exam_attempt_id,
    exam_test_id,
    exam_test_name,
    exam_test_product_id,
    flow_state,
    attempt_initiated_datetime,
    attempt_finished_datetime,
    attempt_duration,
    final_score,
    is_passed,
    mode,
    has_lti_context,
    lti_user_id,
    lti_course_id,
    has_licensed_class_context,
    institution_ccx_id,
    institution_id,
    license_id
from {{ ref("mart_measureup_attempt_context") }}
