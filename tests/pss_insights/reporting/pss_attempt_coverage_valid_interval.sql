{{ config(enabled=var("pss_reporting_enabled", false), tags=["pss_reporting"]) }}

select ccx_id, subsection_block_id
from {{ ref("int_pss_reporting_coverage") }}
where coverage_end <= coverage_start
