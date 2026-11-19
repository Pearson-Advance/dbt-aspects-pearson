{{ config(tags=["pss_insights"], severity="warn") }}

{#
    Subsections with more than one activity-type assignment. The catalog uses
    the most recent one; check the subsection in Studio.
#}
select
    master_course,
    subsection_block_id,
    subsection_name,
    activity_type_value,
    activity_type_assignment_count
from {{ ref("int_pss_activity_type_coverage") }}
where activity_type_assignment_count > 1
