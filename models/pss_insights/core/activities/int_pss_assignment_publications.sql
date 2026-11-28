{{ config(tags=["pss_insights", "pss_insights_activities"]) }}

with
    observations as (
        select
            course_key as master_course,
            parseDateTime64BestEffortOrNull(published_at, 6, 'UTC') as published_time,
            parseDateTime64BestEffortOrNull(
                head_confirmed_at, 6, 'UTC'
            ) as confirmed_time,
            snapshot_id,
            subsections
        from {{ source("pss_event_sink", "pss_subsection_assignment_types") }}
        where
            course_key
            in (select master_course from {{ ref("int_pss_master_course_scope") }})
    ),
    versions as (
        select
            *,
            uniqExact(tuple(published_time)) over (
                partition by master_course, snapshot_id
            ) as version_time_count
        from observations
    ),
    revisions as (
        select
            master_course,
            published_time,
            max(confirmed_time) as head_confirmed_at,
            any(snapshot_id) as version_id,
            any(subsections) as items,
            uniqExact(tuple(snapshot_id, subsections)) as variants,
            max(version_time_count) > 1 as reused_version
        from versions
        group by master_course, published_time
    )
select
    master_course,
    published_time,
    head_confirmed_at,
    if(snapshot_status = 'observed', version_id, '') as snapshot_id,
    if(snapshot_status = 'observed', items, []) as subsections,
    multiIf(
        published_time is null,
        'invalid_publication_time',
        variants != 1 or reused_version,
        'conflicting_publication',
        empty(version_id)
        or arrayExists(
            s -> not startsWith(
                s.subsection_block_id,
                concat(
                    'block-v1:', substring(master_course, 11), '+type@sequential+block@'
                )
            )
            or s.subsection_order = 0
            or s.section_order = 0,
            items
        )
        or arrayUniq(arrayMap(s -> s.subsection_block_id, items)) != length(items)
        or arrayUniq(arrayMap(s -> s.subsection_order, items)) != length(items),
        'invalid_publication',
        'observed'
    ) as snapshot_status,
    row_number() over (
        partition by master_course order by published_time desc nulls first
    )
    = 1 as is_latest
from revisions
