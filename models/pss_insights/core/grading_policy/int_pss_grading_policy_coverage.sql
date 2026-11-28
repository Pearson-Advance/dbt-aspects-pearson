{{ config(tags=["pss_insights", "pss_insights_grading_policy"]) }}

with
    entries as (select * from {{ ref("int_pss_grading_policy_entries") }}),
    masters as (
        select master_course
        from {{ ref("int_pss_master_course_scope") }}
        union distinct
        select master_course
        from entries
    ),
    checks as (
        select
            master_course,
            count() as policy_row_count,
            count() = 4
            and arraySort(groupUniqArray(activity_type))
            = ['Final Exam', 'Lab', 'Practice Exam', 'Quiz'] as has_four_activity_types,
            countIf(isFinite(weight) and weight between 0 and 100) = count()
            and abs(sum(weight) - 100) <= 0.000000001 as weights_valid,
            countIf(
                if(
                    activity_type = 'Final Exam',
                    (
                        final_exam_pass_rule = 'native_pass_mark'
                        and isFinite(final_exam_pass_mark)
                        and final_exam_pass_mark between 0 and 100
                    )
                    or (
                        final_exam_pass_rule = 'measureup_cut_score'
                        and final_exam_pass_mark is null
                    ),
                    final_exam_pass_rule is null and final_exam_pass_mark is null
                )
            )
            = count() as final_rule_valid
        from entries
        group by master_course
    )
select
    m.master_course as master_course,
    coalesce(c.policy_row_count, 0) as policy_row_count,
    coalesce(c.has_four_activity_types, false) as has_four_activity_types,
    coalesce(c.weights_valid, false) as weights_valid,
    coalesce(c.final_rule_valid, false) as final_rule_valid,
    multiIf(
        policy_row_count = 0,
        'missing',
        notEmpty(ifNull(m.master_course, ''))
        and has_four_activity_types
        and weights_valid
        and final_rule_valid,
        'configured',
        'invalid'
    ) as policy_status
from masters as m
left join checks as c on c.master_course = m.master_course
