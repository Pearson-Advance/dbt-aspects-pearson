{{ config(tags=["pss_insights"], severity="warn") }}

{# Master courses with more than one published Final Exam activity. #}
select master_course, count() as final_exam_count
from {{ ref("int_pss_activity_type_coverage") }}
where
    coverage_status in ('classified', 'multiple_tags')
    and in_latest_publish
    and activity_type_value = 'Final Exam'
group by master_course
having final_exam_count > 1
