{{ config(tags=["pss_insights", "pss_insights_grading_policy"]) }}

select
    e.master_course,
    e.activity_type,
    e.weight,
    e.final_exam_pass_rule,
    e.final_exam_pass_mark
from {{ ref("int_pss_grading_policy_entries") }} as e
inner join
    {{ ref("int_pss_grading_policy_coverage") }} as c
    on c.master_course = e.master_course
where c.policy_status = 'configured'
