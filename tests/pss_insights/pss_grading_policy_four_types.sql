{{ config(tags=["pss_insights"]) }}

{# Exactly 4 rows per seeded course, one per activity type. #}
select master_course, count() as row_count, uniqExact(activity_type) as type_count
from {{ ref("int_pss_grading_policy") }}
group by master_course
having row_count != 4 or type_count != 4
