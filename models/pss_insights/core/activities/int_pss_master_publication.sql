{{ config(tags=["pss_insights", "pss_insights_activities"]) }}

with
    structure as (
        select
            course_key,
            1 as observed_structure,
            arraySort(
                groupUniqArrayIf(
                    tuple(block_id, toUInt8(graded)), block_type = 'subsection'
                )
            ) as current_subsections
        from {{ ref("dim_course_blocks") }}
        where
            in_latest_publish
            and course_key
            in (select master_course from {{ ref("int_pss_master_course_scope") }})
        group by course_key
    ),
    overviews as (
        select
            course_key,
            max(
                parseDateTime64BestEffortOrNull(modified, 6, 'UTC')
            ) as overview_modified
        from {{ source("event_sink", "course_overviews") }}
        where
            course_key
            in (select master_course from {{ ref("int_pss_master_course_scope") }})
        group by course_key
    ),
    latest as (
        select * from {{ ref("int_pss_assignment_publications") }} where is_latest
    )
select
    scope.master_course as master_course,
    nullIf(p.snapshot_id, '') as snapshot_id,
    p.published_time as published_time,
    multiIf(
        empty(ifNull(p.snapshot_status, '')),
        'missing_publication',
        p.snapshot_status != 'observed',
        ifNull(p.snapshot_status, 'missing_publication'),
        p.head_confirmed_at is null,
        'unconfirmed_publication',
        o.overview_modified > p.head_confirmed_at,
        'newer_course_overview',
        ifNull(s.observed_structure, 0) = 1
        and s.current_subsections != arraySort(
            arrayMap(
                item -> tuple(item.subsection_block_id, item.graded), p.subsections
            )
        ),
        'structure_disagrees',
        'observed'
    ) as publication_status
from {{ ref("int_pss_master_course_scope") }} as scope
left join latest as p on scope.master_course = p.master_course
left join overviews as o on scope.master_course = o.course_key
left join structure as s on scope.master_course = s.course_key
