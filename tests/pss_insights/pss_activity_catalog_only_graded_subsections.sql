{{ config(tags=["pss_insights"]) }}

select ccx_id, master_course, subsection_block_id
from {{ ref("int_pss_activity_catalog") }}
where
    (master_course, subsection_block_id, snapshot_id) not in (
        select master_course, subsection_block_id, snapshot_id
        from {{ ref("int_pss_subsection_assignments") }}
        where graded and activity_type is not null
    )
