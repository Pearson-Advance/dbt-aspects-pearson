{{ config(tags=["pss_insights"]) }}

with
    classes as (select * from {{ ref("int_pss_class_context") }}),
    units as (
        select distinct course_key as master_course, block_id
        from {{ ref("dim_course_blocks") }}
        where
            block_type = 'unit'
            and in_latest_publish
            and course_key in (select master_course from classes)
    ),
    inventories as (
        select master_course, toNullable(count()) as published_unit_count
        from units
        group by master_course
    ),
    visit_keys as (
        select
            c.ccx_id as ccx_id,
            u.block_id as master_block_id,
            arrayJoin(
                [
                    u.block_id,
                    {{
                        pss_ccx_block_key(
                            "u.block_id", "splitByString('+ccx@', c.ccx_id)[2]"
                        )
                    }}
                ]
            ) as visit_block_id
        from classes as c
        inner join units as u on c.master_course = u.master_course
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
    i.published_unit_count as published_unit_count,
    coalesce(v.observed_visited_units, 0) as observed_visited_unit_count,
    if(
        cov.navigation_complete = 1 and i.published_unit_count > 0,
        coalesce(v.observed_visited_units, 0),
        null
    ) as visited_unit_count,
    visited_unit_count / nullIf(i.published_unit_count, 0) as content_progress
from {{ ref("int_pss_reporting_roster") }} as r
inner join classes as c on r.ccx_id = c.ccx_id
left join inventories as i on c.master_course = i.master_course
left join visits as v on r.ccx_id = v.ccx_id and r.actor_id = v.actor_id
left join {{ ref("int_pss_reporting_class_coverage") }} as cov on r.ccx_id = cov.ccx_id
