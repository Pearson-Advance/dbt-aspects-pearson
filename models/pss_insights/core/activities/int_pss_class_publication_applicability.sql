{{ config(tags=["pss_insights", "pss_insights_activities"]) }}

with
    approvals as (
        select
            master_course,
            ccx_id,
            snapshot_id,
            any(rule) as scope_rule,
            groupUniqArrayIf(
                subsection_block_id, notEmpty(subsection_block_id)
            ) as allowed_subsections,
            uniqExact(rule) = 1
            and multiIf(
                scope_rule = 'inherit_master',
                empty(allowed_subsections),
                scope_rule = 'empty',
                notEmpty(ccx_id)
                and notEmpty(snapshot_id)
                and empty(allowed_subsections),
                scope_rule = 'include_subsection',
                notEmpty(ccx_id)
                and notEmpty(snapshot_id)
                and countIf(empty(subsection_block_id)) = 0,
                false
            ) as valid
        from
            {{
                ref(
                    var(
                        "pss_class_applicability_model",
                        "pss_curriculum_applicability",
                    )
                )
            }}
        group by master_course, ccx_id, snapshot_id
    ),
    candidates as (
        select
            c.ccx_id as ccx_id,
            p.master_course as master_course,
            p.snapshot_id as snapshot_id,
            a.ccx_id as approved_class,
            a.snapshot_id as approved_revision,
            a.scope_rule as scope_rule,
            a.allowed_subsections as allowed_subsections,
            a.valid
            and hasAll(
                arrayMap(s -> s.subsection_block_id, p.subsections),
                a.allowed_subsections
            ) as valid
        from {{ ref("int_pss_class_scope") }} as c
        inner join
            {{ ref("int_pss_assignment_publications") }} as p
            on c.master_course = p.master_course
        inner join approvals as a on a.master_course = p.master_course
        where
            p.snapshot_status = 'observed'
            and (
                a.ccx_id = c.ccx_id
                or (
                    empty(a.ccx_id)
                    and (c.ccx_id, c.master_course) not in (
                        select ccx_id, master_course
                        from approvals
                        where notEmpty(ccx_id)
                    )
                )
            )
            and (a.snapshot_id = p.snapshot_id or empty(a.snapshot_id))
    )
select
    ccx_id,
    master_course,
    snapshot_id,
    if(valid, scope_rule, 'invalid_applicability') as scope_rule,
    allowed_subsections
from candidates
qualify
    row_number() over (
        partition by ccx_id, master_course, snapshot_id
        order by notEmpty(approved_class) desc, notEmpty(approved_revision) desc
    )
    = 1
