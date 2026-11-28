{{ config(tags=["pss_reporting"]) }}

with
    classes as (
        select
            c.ccx_id as ccx_id,
            c.master_course as master_course,
            c.ccx_number as ccx_number,
            a.scope_rule as scope_rule,
            a.allowed_subsections as allowed_subsections
        from {{ ref("int_pss_class_scope") }} as c
        inner join
            {{ ref("int_pss_class_publication_applicability") }} as a
            on c.ccx_id = a.ccx_id
            and c.master_course = a.master_course
        inner join
            {{ ref("int_pss_master_publication") }} as p
            on a.master_course = p.master_course
            and a.snapshot_id = p.snapshot_id
        where
            p.publication_status = 'observed'
            and a.scope_rule in ('inherit_master', 'include_subsection', 'empty')
    ),
    class_trees as (
        select distinct course_key
        from {{ ref("dim_course_blocks") }}
        where in_latest_publish and course_key in (select ccx_id from classes)
    ),
    scoped_structure as (
        select
            c.ccx_id as ccx_id,
            c.ccx_number as ccx_number,
            c.master_course as master_course,
            c.scope_rule as scope_rule,
            c.allowed_subsections as allowed_subsections,
            if(
                empty(ifNull(t.course_key, '')), c.master_course, c.ccx_id
            ) as structure_course
        from classes as c
        left join class_trees as t on c.ccx_id = t.course_key
    ),
    units as (
        select distinct
            c.ccx_id as ccx_id,
            c.ccx_number as ccx_number,
            concat(
                'block-v1:',
                substring(c.master_course, 11),
                substring(b.block_id, position(b.block_id, '+type@'))
            ) as master_block_id
        from scoped_structure as c
        inner join
            {{ ref("dim_course_blocks") }} as b on c.structure_course = b.course_key
        where
            b.block_type = 'unit'
            and b.in_latest_publish
            and (
                startsWith(
                    b.block_id,
                    concat(
                        'block-v1:',
                        substring(c.master_course, 11),
                        '+type@vertical+block@'
                    )
                )
                or startsWith(
                    b.block_id,
                    concat(
                        'ccx-block-v1:', substring(c.ccx_id, 8), '+type@vertical+block@'
                    )
                )
            )
            and (
                c.scope_rule = 'inherit_master'
                or (
                    c.scope_rule = 'include_subsection'
                    and has(
                        c.allowed_subsections,
                        concat(
                            'block-v1:',
                            substring(c.master_course, 11),
                            substring(
                                b.subsection_block_id,
                                position(b.subsection_block_id, '+type@')
                            )
                        )
                    )
                )
            )
    ),
    inventories as (
        select ccx_id, toNullable(count()) as published_unit_count
        from units
        group by ccx_id
    ),
    visit_keys as (
        select
            ccx_id,
            master_block_id,
            arrayJoin(
                [
                    master_block_id,
                    {{ pss_ccx_block_key("master_block_id", "ccx_number") }}
                ]
            ) as visit_block_id
        from units
    ),
    visits as (
        select
            k.ccx_id as ccx_id,
            v.actor_id as actor_id,
            uniqExact(k.master_block_id) as observed_visited_units
        from {{ ref("fact_learner_page_visits") }} as v
        inner join
            visit_keys as k on v.course_key = k.ccx_id and v.block_id = k.visit_block_id
        group by k.ccx_id, v.actor_id
    )
select
    r.ccx_id as ccx_id,
    r.actor_id as actor_id,
    if(c.scope_rule = 'empty', 0, i.published_unit_count) as published_unit_count,
    ifNull(v.observed_visited_units, 0) as observed_visited_unit_count,
    if(
        cov.navigation_complete = 1 and published_unit_count > 0,
        observed_visited_unit_count,
        null
    ) as visited_unit_count,
    visited_unit_count / nullIf(published_unit_count, 0) as content_progress
from {{ ref("int_pss_reporting_roster") }} as r
left join classes as c on r.ccx_id = c.ccx_id
left join inventories as i on r.ccx_id = i.ccx_id
left join visits as v on r.ccx_id = v.ccx_id and r.actor_id = v.actor_id
left join {{ ref("int_pss_reporting_class_coverage") }} as cov on r.ccx_id = cov.ccx_id
