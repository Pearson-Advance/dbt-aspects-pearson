{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(coalesce(institution_id, ''), ccx_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [
                target.schema ~ ".dim_course_blocks_mv",
            ],
            "depends_on_validation": true,
        },
        tags=["pss_reporting"],
    )
}}

with
    listed as (
        select institution_id, ccx_id, snapshot_id, count() as activities
        from {{ ref("int_pss_activity_catalog") }}
        where in_latest_publish
        group by institution_id, ccx_id, snapshot_id
    )
select
    i.institution_id as institution_id,
    i.ccx_id as ccx_id,
    i.master_course as master_course,
    i.snapshot_id as snapshot_id,
    i.inventory_status as inventory_status,
    ifNull(a.activities, 0) as known_published_activities,
    multiIf(
        i.inventory_status = 'classified' and known_published_activities > 0,
        'complete',
        i.inventory_status = 'no_graded_subsections' and known_published_activities = 0,
        'complete',
        i.inventory_status in (
            'classified',
            'no_graded_subsections',
            'partially_classified',
            'graded_not_classified'
        ),
        'incomplete',
        'unavailable'
    ) as catalog_status,
    r.roster_status as roster_status,
    r.registered_source_learners as registered_source_learners,
    r.resolved_registered_learners as resolved_registered_learners,
    r.unresolved_registered_learners as unresolved_registered_learners,
    r.registered_actor_denominator as registered_actor_denominator
from {{ ref("int_pss_class_inventory_coverage") }} as i
inner join {{ ref("int_pss_roster_coverage") }} as r on r.ccx_id = i.ccx_id
left join
    listed as a
    on i.institution_id = a.institution_id
    and i.ccx_id = a.ccx_id
    and i.snapshot_id = a.snapshot_id
