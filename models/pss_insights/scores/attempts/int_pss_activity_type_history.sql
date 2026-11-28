{{ config(tags=["pss_scores"]) }}

-- Compute the end before filtering revisions or flattening their activities.
-- Even an empty, withheld or conflicting next revision ends the prior type.
with
    revisions as (
        select
            master_course,
            snapshot_id,
            snapshot_status,
            published_time,
            is_latest,
            countIf(published_time is null) over (
                partition by master_course
            ) as undated_revisions,
            leadInFrame(toNullable(published_time), 1, null) over (
                partition by master_course
                order by published_time
                rows between current row and unbounded following
            ) as next_publication
        from {{ ref("int_pss_assignment_publications") }}
    )
select
    assumeNotNull(r.ccx_id) as ccx_id,
    a.master_course as master_course,
    assumeNotNull(a.subsection_block_id) as subsection_block_id,
    v.snapshot_id as snapshot_id,
    assumeNotNull(v.published_time) as valid_from,
    v.next_publication as valid_until,
    assumeNotNull(a.activity_type) as activity_type
from revisions as v
inner join
    {{ ref("int_pss_subsection_assignments") }} as a
    on v.master_course = a.master_course
    and v.snapshot_id = a.snapshot_id
inner join
    {{ ref("int_pss_class_publication_applicability") }} as r
    on v.master_course = r.master_course
    and v.snapshot_id = r.snapshot_id
inner join
    {{ ref("int_pss_master_publication") }} as head
    on head.master_course = v.master_course
where
    v.snapshot_status = 'observed'
    and v.undated_revisions = 0
    and (not v.is_latest or head.publication_status = 'observed')
    and a.graded
    and a.activity_type is not null
    and (
        r.scope_rule = 'inherit_master'
        or (
            r.scope_rule = 'include_subsection'
            and has(r.allowed_subsections, a.subsection_block_id)
        )
    )
