{{ config(tags=["pss_insights"]) }}

{# Weights are 0-100 and sum to 100 per course. #}
select
    master_course,
    sum(weight) as total_weight,
    min(weight) as min_weight,
    max(weight) as max_weight
from {{ ref("int_pss_grading_policy") }}
group by master_course
having abs(total_weight - 100) > 0.001 or min_weight < 0 or max_weight > 100
