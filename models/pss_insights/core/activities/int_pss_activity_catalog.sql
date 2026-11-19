{{ config(tags=["pss_insights", "pss_insights_activities"]) }}

{#
    Graded subsections of each class's master course whose activity type is
    recognized, crossed with the classes. Subsections no longer in the latest
    publish stay, so old attempts still resolve.

    activity_sequence is computed once per master course (every class of a
    course shares it) over published activities only.

    Section columns are null for subsections no longer in the latest publish:
    dim_course_blocks resolves sections by position among the published ones,
    so for a deleted subsection they may point to the wrong section.
#}
with
    classes as (
        select * from {{ ref("int_pss_class_scope") }} where master_course is not null
    ),
    subsections as (
        select
            course_key as master_course,
            block_id as subsection_block_id,
            block_name as subsection_name,
            if(
                in_latest_publish, nullIf(section_block_id, ''), null
            ) as section_block_id,
            if(in_latest_publish, section_course_order, null) as section_course_order,
            course_order as subsection_course_order,
            in_latest_publish
        from {{ ref("dim_course_blocks") }}
        where
            block_type = 'subsection'
            and graded
            and course_key
            in (select master_course from {{ ref("int_pss_master_course_scope") }})
    ),
    activities as (
        select
            subsections.*,
            activity_type.activity_type_value as activity_type,
            activity_type.activity_type_object_tag_id as activity_type_object_tag_id
        from subsections
        inner join
            {{ ref("int_pss_object_activity_type") }} as activity_type
            on activity_type.object_id = subsections.subsection_block_id
        where activity_type.is_recognized_value
    ),
    sequenced as (
        select
            activities.*,
            if(
                in_latest_publish,
                row_number() over (
                    partition by master_course, in_latest_publish
                    order by subsection_course_order, subsection_block_id
                ),
                null
            ) as activity_sequence
        from activities
    )
select
    classes.institution_ccx_id as institution_ccx_id,
    classes.institution_id as institution_id,
    classes.license_id as license_id,
    classes.license_status as license_status,
    classes.ccx_id as ccx_id,
    classes.class_name as class_name,
    sequenced.master_course as master_course,
    sequenced.subsection_block_id as subsection_block_id,
    {{ pss_ccx_block_key("sequenced.subsection_block_id", "classes.ccx_number") }}
    as ccx_subsection_block_id,
    sequenced.subsection_name as subsection_name,
    sequenced.section_block_id as section_block_id,
    sequenced.section_course_order as section_course_order,
    sequenced.subsection_course_order as subsection_course_order,
    sequenced.activity_sequence as activity_sequence,
    sequenced.activity_type as activity_type,
    sequenced.in_latest_publish as in_latest_publish,
    sequenced.activity_type_object_tag_id as activity_type_object_tag_id
from classes
inner join sequenced on sequenced.master_course = classes.master_course
