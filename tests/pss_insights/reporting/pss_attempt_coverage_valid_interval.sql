{{ config(tags=["pss_reporting"]) }}

select ccx_id, subsection_block_id
from {{ ref("int_pss_reporting_coverage") }}
where
    if(
        full_history,
        coverage_start is not null or coverage_end is not null,
        coverage_start is null or coverage_end is null or coverage_end <= coverage_start
    )
