{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(master_course, problem_block_id, observed_at)",
        refreshable={"interval": "EVERY 4 HOUR"},
        tags=["pss_insights", "pss_insights_subdomains"],
    )
}}

-- Every export has a header, including exports with no problems. Select the
-- header as of the response first: a later export can omit a removed problem.
with
    scoped as (
        select
            course_key as master_course,
            parseDateTime64BestEffortOrNull(time_last_dumped, 6, 'UTC') as dumped_at,
            dump_id,
            location,
            xblock_data_json,
            startsWith(location, 'block-v1:')
            and {{ pss_course_key_from_block_key("location") }} = course_key
            and (
                position(location, '+type@problem+block@') > 0
                or position(location, '+type@sequential+block@') > 0
            ) as is_relevant_block
        from {{ source("event_sink", "course_blocks") }}
        where
            course_key
            in (select master_course from {{ ref("int_pss_master_course_scope") }})
    ),
    blocks as (
        select
            master_course,
            dumped_at,
            dump_id,
            arrayJoin(if(is_relevant_block, ['', location], [''])) as block_id,
            minIf(xblock_data_json, block_id != '') as payload,
            payload != maxIf(xblock_data_json, block_id != '') as conflicting_payload
        from scoped
        group by master_course, dumped_at, dump_id, block_id
    ),
    clocks as (
        select
            *,
            countIf(dumped_at is null) over (partition by master_course) > 0
            or min(dumped_at) over dump_clock != max(dumped_at) over dump_clock
            or dump_id
            = toUUID('00000000-0000-0000-0000-000000000000') as uncertain_clock
        from blocks
        window dump_clock as (partition by master_course, dump_id)
    ),
    versions as (
        select
            master_course,
            assumeNotNull(dumped_at) as observed_at,
            block_id,
            min(dump_id) as first_id,
            first_id != max(dump_id) or max(uncertain_clock) as conflicting_export,
            any(clocks.payload) as payload,
            max(clocks.conflicting_payload) as conflicting_payload
        from clocks
        where dumped_at is not null
        group by master_course, observed_at, block_id
    ),
    positions as (
        select
            *,
            position(block_id, '+type@sequential+block@') > 0 as is_subsection,
            if(
                JSONType(payload, 'section') in ('Int64', 'UInt64'),
                JSONExtractUInt(payload, 'section'),
                0
            ) as section_number,
            if(
                JSONType(payload, 'subsection') in ('Int64', 'UInt64'),
                JSONExtractUInt(payload, 'subsection'),
                0
            ) as subsection_number,
            max(conflicting_export) over export as ambiguous_capture,
            max(is_subsection and conflicting_payload) over export
            as conflicting_structure,
            maxIf(observed_at, observed_at <= now64(6, 'UTC')) over (
                partition by master_course
            ) as latest_at
        from versions
        window export as (partition by master_course, observed_at)
    ),
    checked as (
        select
            *,
            minIf(block_id, is_subsection) over position_in_export as first_subsection,
            maxIf(block_id, is_subsection) over position_in_export as last_subsection,
            JSONExtractArrayRaw(payload, 'tags') as raw_tags,
            multiIf(
                ambiguous_capture,
                'ambiguous_capture',
                block_id = '',
                'observed',
                conflicting_payload,
                'conflicting_payload',
                not isValidJSON(payload)
                or JSONType(payload, 'tags') != 'Array'
                or not arrayAll(
                    t -> JSONType(t) in ('Int64', 'UInt64')
                    and JSONExtractUInt(t) between 1 and 2147483647,
                    raw_tags
                ),
                'tags_unavailable',
                'observed'
            ) as capture_status
        from positions
        window
            position_in_export as (
                partition by
                    master_course, observed_at, section_number, subsection_number
            )
    )
select
    master_course,
    observed_at,
    block_id as problem_block_id,
    if(ambiguous_capture, null, first_id) as capture_id,
    observed_at = latest_at as is_latest_capture,
    if(
        not ambiguous_capture
        and not conflicting_payload
        and not conflicting_structure
        and section_number > 0
        and subsection_number > 0
        and first_subsection = last_subsection,
        nullIf(first_subsection, ''),
        null
    ) as subsection_block_id,
    capture_status,
    if(
        capture_status = 'observed',
        arraySort(arrayDistinct(arrayMap(t -> JSONExtractUInt(t), raw_tags))),
        []
    ) as tag_ids
from checked
where not is_subsection
