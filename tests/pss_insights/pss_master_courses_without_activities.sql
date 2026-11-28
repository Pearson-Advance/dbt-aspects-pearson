{{ config(tags=["pss_insights"], severity="warn") }}

select master_course, inventory_status, observed_graded_subsections
from {{ ref("int_pss_master_inventory_coverage") }}
where inventory_status != 'classified'
