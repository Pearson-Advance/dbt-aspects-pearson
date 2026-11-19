{{ config(tags=["pss_insights", "pss_insights_activities"]) }}

{#
    coverage_status is checked in this order, so each subsection gets one:
      tagged_not_graded   tagged, but the subsection is not graded
      untagged            graded, no activity-type tag
      unrecognized_value  graded, latest tag is not an accepted value
      multiple_tags       graded, several assignments; the catalog uses the
                          most recent one
      classified          graded, one accepted tag

    The component profile counts blocks under the subsection in the latest
    publish (dim_course_subsection_items only counts unit/problem/video, so it
    can't show LTI or Skillable content). html and video are content, not
    gradable components, so they are not counted.
#}
with
    blocks as (
        select *
        from {{ ref("dim_course_blocks") }}
        where
            course_key
            in (select master_course from {{ ref("int_pss_master_course_scope") }})
    ),
    subsections as (
        select
            course_key as master_course,
            block_id as subsection_block_id,
            block_name as subsection_name,
            course_order as subsection_course_order,
            graded,
            in_latest_publish
        from blocks
        where block_type = 'subsection'
    ),
    components as (
        select
            subsection_block_id,
            countIf(block_type = 'problem') as problem_count,
            countIf(block_type = 'lti_consumer') as lti_consumer_count,
            countIf(block_type = 'skillable_xblock') as skillable_xblock_count,
            countIf(
                block_type
                not in ('problem', 'lti_consumer', 'skillable_xblock', 'html', 'video')
            ) as other_component_count
        from blocks
        where in_latest_publish and block_type not in ('section', 'subsection', 'unit')
        group by subsection_block_id
    )
select
    subsections.master_course as master_course,
    subsections.subsection_block_id as subsection_block_id,
    subsections.subsection_name as subsection_name,
    subsections.subsection_course_order as subsection_course_order,
    subsections.graded as graded,
    subsections.in_latest_publish as in_latest_publish,
    activity_type.activity_type_value as activity_type_value,
    activity_type.activity_type_object_tag_id as activity_type_object_tag_id,
    coalesce(
        activity_type.activity_type_assignment_count, 0
    ) as activity_type_assignment_count,
    multiIf(
        activity_type.object_id is not null and not subsections.graded,
        'tagged_not_graded',
        activity_type.object_id is null,
        'untagged',
        not activity_type.is_recognized_value,
        'unrecognized_value',
        activity_type.activity_type_assignment_count > 1,
        'multiple_tags',
        'classified'
    ) as coverage_status,
    coalesce(components.problem_count, 0) as problem_count,
    coalesce(components.lti_consumer_count, 0) as lti_consumer_count,
    coalesce(components.skillable_xblock_count, 0) as skillable_xblock_count,
    coalesce(components.other_component_count, 0) as other_component_count
from subsections
left join
    {{ ref("int_pss_object_activity_type") }} as activity_type
    on activity_type.object_id = subsections.subsection_block_id
left join components on components.subsection_block_id = subsections.subsection_block_id
where
    subsections.graded or activity_type.object_id is not null
    settings join_use_nulls = 1
