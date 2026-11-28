{{ config(tags=["pss_insights", "pss_insights_activities"]) }}

-- Resolve the last applicable row for the class before filtering grading or
-- mapping. A CCX can stop using an item while the master continues publishing it.
with
    applicable as (
        select r.ccx_id as applicable_ccx_id, a.*
        from {{ ref("int_pss_subsection_assignments") }} as a
        inner join
            {{ ref("int_pss_class_publication_applicability") }} as r
            on r.master_course = a.master_course
            and r.snapshot_id = a.snapshot_id
        where
            r.scope_rule = 'inherit_master'
            or (
                r.scope_rule = 'include_subsection'
                and has(r.allowed_subsections, a.subsection_block_id)
            )
        qualify
            row_number() over (
                partition by r.ccx_id, a.master_course, a.subsection_block_id
                order by a.published_time desc
            )
            = 1
    )
select
    c.institution_ccx_id as institution_ccx_id,
    c.institution_id as institution_id,
    c.license_id as license_id,
    c.license_status as license_status,
    c.ccx_id as ccx_id,
    c.class_name as class_name,
    a.master_course as master_course,
    a.subsection_block_id as subsection_block_id,
    {{ pss_ccx_block_key("a.subsection_block_id", "c.ccx_number") }}
    as ccx_subsection_block_id,
    a.subsection_name as subsection_name,
    if(a.in_latest_publish, a.section_block_id, null) as section_block_id,
    if(a.in_latest_publish, a.section_name, null) as section_name,
    if(a.in_latest_publish, a.section_course_order, null) as section_course_order,
    a.subsection_course_order as subsection_course_order,
    if(
        a.in_latest_publish,
        row_number() over (
            partition by c.ccx_id, a.in_latest_publish
            order by a.subsection_course_order, a.subsection_block_id
        ),
        null
    ) as activity_sequence,
    assumeNotNull(a.activity_type) as activity_type,
    a.assignment_type as assignment_type,
    a.snapshot_id as snapshot_id,
    a.in_latest_publish as in_latest_publish
from {{ ref("int_pss_class_scope") }} as c
inner join
    applicable as a
    on c.ccx_id = a.applicable_ccx_id
    and c.master_course = a.master_course
-- Keep current activities while a publication awaits verification so attempts stay
-- linkable; inventory coverage reports the publication status to consumers.
where a.graded and a.activity_type is not null
