{{ config(tags=["pss_insights", "pss_insights_activities"]) }}

with
    blocks as (
        select
            master_course,
            snapshot_id,
            published_time,
            item.subsection_block_id as subsection_block_id,
            item.assignment_type as assignment_type,
            item.graded as graded,
            item.subsection_name as subsection_name,
            item.section_block_id as section_block_id,
            item.section_name as section_name,
            item.section_order as section_course_order,
            item.subsection_order as subsection_course_order,
            item.problem_count as problem_count,
            item.lti_consumer_count as lti_consumer_count,
            item.skillable_xblock_count as skillable_xblock_count,
            item.other_component_count as other_component_count,
            is_latest as in_latest_publish
        from {{ ref("int_pss_assignment_publications") }} array
        join subsections as item
        where snapshot_status = 'observed'
    ),
    mappings as (
        select master_course, assignment_type, any(activity_type) as mapped_type
        from {{ ref("pss_assignment_type_mapping") }}
        group by master_course, assignment_type
        having
            count() = 1
            and mapped_type in ('Quiz', 'Lab', 'Practice Exam', 'Final Exam')
            and notEmpty(assignment_type)
    )
select b.*, nullIf(m.mapped_type, '') as activity_type
from blocks as b
left join
    mappings as m
    on m.master_course = b.master_course
    and m.assignment_type = b.assignment_type
