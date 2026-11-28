{{ config(tags=["pss_insights", "pss_insights_classes"]) }}

{# Master courses with at least one in-scope class. #}
select distinct master_course
from {{ ref("int_pss_class_scope") }}
where master_course is not null
