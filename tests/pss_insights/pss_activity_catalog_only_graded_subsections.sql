{{ config(tags=["pss_insights"]) }}

{# Every catalog row is a graded subsection of its master course. #}
select catalog.ccx_id, catalog.subsection_block_id
from {{ ref("int_pss_activity_catalog") }} as catalog
left join
    {{ ref("dim_course_blocks") }} as blocks
    on blocks.course_key = catalog.master_course
    and blocks.block_id = catalog.subsection_block_id
    and blocks.block_type = 'subsection'
    and blocks.graded
where blocks.block_id is null settings join_use_nulls = 1
