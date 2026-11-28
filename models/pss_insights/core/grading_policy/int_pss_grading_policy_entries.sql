{{ config(tags=["pss_insights", "pss_insights_grading_policy"]) }}

select
    trim(master_course) as master_course,
    trim(activity_type) as activity_type,
    weight,
    nullIf(trim(coalesce(final_exam_pass_rule, '')), '') as final_exam_pass_rule,
    final_exam_pass_mark
from {{ ref("pss_grading_policy") }}
