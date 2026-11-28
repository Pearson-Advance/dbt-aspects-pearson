{{ config(tags=["pss_insights", "pss_insights_activities"]) }}

with
    latest_assignment as (
        select *
        from {{ ref("int_pss_subsection_assignments") }}
        qualify
            row_number() over (
                partition by master_course, subsection_block_id
                order by published_time desc
            )
            = 1
    ),
    reported as (
        select
            a.master_course as master_course,
            a.subsection_block_id as subsection_block_id,
            a.subsection_name as subsection_name,
            a.graded as graded,
            a.in_latest_publish as in_latest_publish,
            a.snapshot_id as snapshot_id,
            a.assignment_type as assignment_type,
            a.activity_type as activity_type,
            a.section_block_id as section_block_id,
            a.section_name as section_name,
            a.section_course_order as section_course_order,
            a.subsection_course_order as subsection_course_order,
            a.problem_count as problem_count,
            a.lti_consumer_count as lti_consumer_count,
            a.skillable_xblock_count as skillable_xblock_count,
            a.other_component_count as other_component_count
        from latest_assignment as a
        inner join
            {{ ref("int_pss_master_publication") }} as p
            on p.master_course = a.master_course
        where
            a.graded and (not a.in_latest_publish or p.publication_status = 'observed')
    ),
    -- Structure still identifies graded gaps before a complete matching Studio
    -- snapshot arrives. Its metadata cannot identify the assignment type.
    structural_gaps as (
        select
            b.course_key as master_course,
            b.block_id as subsection_block_id,
            b.block_name as subsection_name,
            b.graded as graded,
            b.in_latest_publish as in_latest_publish,
            null as snapshot_id,
            null as assignment_type,
            null as activity_type,
            null as section_block_id,
            null as section_name,
            null as section_course_order,
            b.course_order as subsection_course_order,
            null as problem_count,
            null as lti_consumer_count,
            null as skillable_xblock_count,
            null as other_component_count
        from {{ ref("dim_course_blocks") }} as b
        inner join
            {{ ref("int_pss_master_publication") }} as p
            on p.master_course = b.course_key
        where
            b.block_type = 'subsection'
            and b.graded
            and (p.publication_status != 'observed' or not b.in_latest_publish)
            and (
                p.publication_status != 'observed'
                or (b.course_key, b.block_id) not in (
                    select master_course, subsection_block_id
                    from latest_assignment
                    where not graded
                )
            )
    ),
    evidence as (
        select *
        from reported
        union all
        select *
        from structural_gaps
    )
select
    *,
    multiIf(
        snapshot_id is null,
        'assignment_type_unavailable',
        activity_type is null,
        'unmapped_assignment_type',
        'classified'
    ) as coverage_status
from evidence
-- Current structural evidence wins over a previously classified revision. For
-- historical rows, prefer the observed Studio state to a structural placeholder.
qualify
    row_number() over (
        partition by master_course, subsection_block_id
        order by in_latest_publish desc, (snapshot_id is not null) desc
    )
    = 1
