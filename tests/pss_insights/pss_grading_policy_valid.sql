{{ config(tags=["pss_insights"]) }}

select *
from {{ ref("int_pss_grading_policy_coverage") }}
where policy_status = 'invalid'
