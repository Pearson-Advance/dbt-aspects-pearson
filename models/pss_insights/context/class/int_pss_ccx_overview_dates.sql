{{ config(tags=["pss_context"]) }}

{# Each overview already contains the LMS-resolved bounds for its own CCX. #}
with
    exports as (
        select
            course_key,
            course_start,
            course_end,
            time_last_dumped,
            dump_id,
            parseDateTime64BestEffortOrNull(modified, 6, 'UTC') as modified_at,
            parseDateTime64BestEffortOrNull(time_last_dumped, 6, 'UTC') as dumped_at,
            modified_at is not null and dumped_at is not null as orderable
        from {{ source("event_sink", "course_overviews") }}
        where startsWith(course_key, 'ccx-v1:')
    ),
    latest as (
        select
            course_key as ccx_id,
            argMaxIf(
                tuple(course_start, course_end, time_last_dumped, modified_at),
                tuple(modified_at, dumped_at, dump_id),
                orderable
            ) as bounds,
            -- A valid source revision after the last uncertain timestamp supersedes it.
            maxOrNullIf(
                coalesce(modified_at, dumped_at), not orderable
            ) as uncertain_through,
            countIf(modified_at is null and dumped_at is null) > 0
            or countIf(orderable) = 0
            or ifNull(uncertain_through >= bounds .4, false) as unorderable_overview
        from exports
        group by course_key
    ),
    parsed as (
        select
            ccx_id,
            bounds .1 as raw_start,
            bounds .2 as raw_end,
            bounds .3 as overview_dumped_at,
            unorderable_overview,
            parseDateTime64BestEffortOrNull(raw_start, 6, 'UTC') as start_at,
            parseDateTime64BestEffortOrNull(raw_end, 6, 'UTC') as end_at
        from latest
    ),
    dated as (
        select
            *,
            toDate32(
                start_at, '{{ var("pss_reporting_timezone", "UTC") }}'
            ) as start_day,
            toDate32(end_at, '{{ var("pss_reporting_timezone", "UTC") }}') as end_day,
            start_day between '1970-01-01' and '2149-06-06' as start_representable,
            end_day between '1970-01-01' and '2149-06-06' as end_representable,
            case
                when unorderable_overview
                then 'unorderable_overview'
                when coalesce(raw_start, '') in ('', 'None', 'NULL')
                then 'missing_start'
                when coalesce(raw_end, '') in ('', 'None', 'NULL')
                then 'missing_end'
                when ifNull(start_representable, 0) = 0
                then 'invalid_start'
                when ifNull(end_representable, 0) = 0
                then 'invalid_end'
                when end_at < start_at
                then 'inverted_bounds'
                else 'available'
            end as class_date_status
        from parsed
    )
select
    ccx_id,
    if(
        start_representable
        and class_date_status not in ('unorderable_overview', 'inverted_bounds'),
        toDate(start_day),
        null
    ) as class_start,
    if(
        end_representable
        and class_date_status not in ('unorderable_overview', 'inverted_bounds'),
        toDate(end_day),
        null
    ) as class_end,
    class_date_status,
    overview_dumped_at
from dated
