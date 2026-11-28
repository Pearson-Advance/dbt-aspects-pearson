{{ config(tags=["pss_insights"], severity="warn") }}

{#
    In-scope master courses with graded subsections in the latest publish but
    no grading policy. Courses without graded content need no policy.
#}
select distinct master_course
from {{ ref("int_pss_activity_type_coverage") }}
where
    graded
    and in_latest_publish
    and master_course
    not in (select master_course from {{ ref("int_pss_grading_policy") }})
