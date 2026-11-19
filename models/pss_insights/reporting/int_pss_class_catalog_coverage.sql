{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(coalesce(institution_id, ''), ccx_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": pss_reporting_upstream_mvs("catalog"),
        },
        tags=["pss_insights", "pss_reporting"],
    )
}}

with
    published as (
        select
            b.course_key as master_course,
            b.block_id as subsection_block_id,
            b.graded as graded,
            coalesce(c.coverage_status, '') as coverage_status
        from {{ ref("dim_course_blocks") }} as b
        left join
            (
                select *
                from {{ ref("int_pss_activity_type_coverage") }}
                where in_latest_publish
            ) as c
            on c.master_course = b.course_key
            and c.subsection_block_id = b.block_id
        where
            b.in_latest_publish
            and b.block_type = 'subsection'
            and b.course_key
            in (select master_course from {{ ref("int_pss_class_context") }})
    ),
    structure as (
        select
            master_course,
            count() as published_subsections,
            countIf(graded) as graded_subsections,
            countIf(
                (graded and coverage_status != 'classified')
                or (not graded and coverage_status != '')
            ) as unresolved_subsections
        from published
        group by master_course
    ),
    inventory as (
        select
            a.ccx_id as ccx_id,
            a.master_course as master_course,
            count() as known_published_activities,
            countIf(coalesce(p.coverage_status, '') != 'classified') as unverified
        from {{ ref("int_pss_activity_catalog") }} as a
        left join
            published as p
            on p.master_course = a.master_course
            and p.subsection_block_id = a.subsection_block_id
        where a.in_latest_publish
        group by a.ccx_id, a.master_course
    )
select
    c.ccx_id as ccx_id,
    c.institution_id as institution_id,
    c.master_course as master_course,
    coalesce(i.known_published_activities, 0) as known_published_activities,
    multiIf(
        coalesce(s.published_subsections, 0) = 0,
        'unavailable',
        s.unresolved_subsections > 0
        or coalesce(i.unverified, 0) > 0
        or coalesce(i.known_published_activities, 0) != s.graded_subsections,
        'incomplete',
        'complete'
    ) as catalog_status
from {{ ref("int_pss_class_context") }} as c
left join structure as s on s.master_course = c.master_course
left join inventory as i on i.ccx_id = c.ccx_id and i.master_course = c.master_course
