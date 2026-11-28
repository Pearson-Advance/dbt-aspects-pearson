{{ config(tags=["pss_insights"], severity="warn") }}

select master_course, count() as final_exam_count
from {{ ref("int_pss_activity_type_coverage") }}
where in_latest_publish and activity_type = 'Final Exam'
group by master_course
having final_exam_count > 1
