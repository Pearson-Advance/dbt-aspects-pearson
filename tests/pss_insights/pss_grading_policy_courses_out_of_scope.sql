{{ config(tags=["pss_insights"], severity="warn") }}

{# Seeded courses that are not in scope; usually a typo in the seed. #}
select distinct master_course
from {{ ref("int_pss_grading_policy_entries") }}
where
    master_course
    not in (select master_course from {{ ref("int_pss_master_course_scope") }})
