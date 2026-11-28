{{ config(tags=["pss_insights", "pss_insights_activities"]) }}

with
    current_scope as (
        select
            c.ccx_id as ccx_id,
            c.institution_id as institution_id,
            c.master_course as master_course,
            p.snapshot_id as snapshot_id,
            coalesce(
                nullIf(p.publication_status, ''), 'unresolved_master'
            ) as publication_status,
            coalesce(
                nullIf(r.scope_rule, ''), 'applicability_unavailable'
            ) as applicability_status,
            r.allowed_subsections as allowed_subsections
        from {{ ref("int_pss_class_scope") }} as c
        left join
            {{ ref("int_pss_master_publication") }} as p
            on c.master_course = p.master_course
        left join
            {{ ref("int_pss_class_publication_applicability") }} as r
            on c.ccx_id = r.ccx_id
            and p.master_course = r.master_course
            and p.snapshot_id = r.snapshot_id
    ),
    counts as (
        select
            c.ccx_id as ccx_id,
            count() as graded_count,
            countIf(a.coverage_status = 'classified') as classified_count
        from current_scope as c
        inner join
            {{ ref("int_pss_activity_type_coverage") }} as a
            on c.master_course = a.master_course
        where
            a.in_latest_publish
            and (
                c.applicability_status = 'inherit_master'
                or (
                    c.applicability_status = 'include_subsection'
                    and has(c.allowed_subsections, a.subsection_block_id)
                )
            )
        group by c.ccx_id
    )
select
    c.ccx_id as ccx_id,
    c.institution_id as institution_id,
    c.master_course as master_course,
    c.snapshot_id as snapshot_id,
    c.publication_status as publication_status,
    c.applicability_status as applicability_status,
    multiIf(
        c.publication_status != 'observed',
        c.publication_status,
        c.applicability_status not in ('inherit_master', 'include_subsection', 'empty'),
        c.applicability_status,
        ifNull(n.graded_count, 0) = 0,
        'no_graded_subsections',
        ifNull(n.classified_count, 0) = 0,
        'graded_not_classified',
        n.classified_count != n.graded_count,
        'partially_classified',
        'classified'
    ) as inventory_status
from current_scope as c
left join counts as n on c.ccx_id = n.ccx_id
