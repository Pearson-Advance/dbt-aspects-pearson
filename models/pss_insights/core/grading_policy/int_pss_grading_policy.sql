{{ config(tags=["pss_insights", "pss_insights_grading_policy"]) }}

{#
    The only reader of the pss_grading_policy seed: if the weights move out of
    the seed, only this select changes. Values are trimmed and an empty pass
    rule becomes null, so hand-edited CSV cells don't break the tests.
#}
select
    trim(master_course) as master_course,
    trim(activity_type) as activity_type,
    toFloat64(weight) as weight,
    nullIf(trim(coalesce(final_exam_pass_rule, '')), '') as final_exam_pass_rule,
    final_exam_pass_mark
from {{ ref("pss_grading_policy") }}
