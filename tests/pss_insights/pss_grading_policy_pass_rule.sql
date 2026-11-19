{{ config(tags=["pss_insights"]) }}

{#
    final_exam_pass_rule is set on the Final Exam row only, and always there;
    final_exam_pass_mark (0-100) is set if and only if the rule is
    native_pass_mark. coalesce() keeps a null rule from hiding a violation.
#}
select master_course, activity_type, final_exam_pass_rule, final_exam_pass_mark
from {{ ref("int_pss_grading_policy") }}
where
    (activity_type = 'Final Exam') != (final_exam_pass_rule is not null)
    or coalesce(final_exam_pass_rule = 'native_pass_mark', false)
    != (final_exam_pass_mark is not null)
    or final_exam_pass_mark < 0
    or final_exam_pass_mark > 100
