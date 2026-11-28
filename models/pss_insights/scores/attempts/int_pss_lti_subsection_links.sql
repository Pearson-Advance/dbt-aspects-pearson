{{ config(tags=["pss_scores"]) }}

-- Resolve each LTI block in its own exported course tree. A removed block's
-- old positions can point to a different subsection in the latest tree.
with
    blocks as (
        select distinct
            course_key,
            dump_id,
            location,
            parseDateTime64BestEffortOrNull(time_last_dumped, 6, 'UTC') as observed_at,
            JSONExtractInt(xblock_data_json, 'section') as section_position,
            JSONExtractInt(xblock_data_json, 'subsection') as subsection_position
        from {{ source("event_sink", "course_blocks") }}
        where
            course_key
            in (select master_course from {{ ref("int_pss_master_course_scope") }})
            and (
                position(location, '+type@sequential+block@') > 0
                or position(location, '+type@lti_consumer+block@') > 0
                or position(location, '+type@lti+block@') > 0
            )
            and startsWith(
                location, concat('block-v1:', substring(course_key, 11), '+type@')
            )
    ),
    parents as (
        select
            course_key,
            dump_id,
            section_position,
            subsection_position,
            uniqExact(location) as parent_count,
            min(location) as candidate_parent,
            minOrNull(observed_at) as parent_observed_at
        from blocks
        where
            position(location, '+type@sequential+block@') > 0
            and section_position > 0
            and subsection_position > 0
        group by course_key, dump_id, section_position, subsection_position
    ),
    observations as (
        select
            b.course_key as master_course,
            b.location as master_block_id,
            groupUniqArrayIf(
                p.candidate_parent, p.parent_count = 1
            ) as observed_parents,
            countIf(ifNull(p.parent_count, 0) = 0) as incomplete_dumps,
            countIf(p.parent_count > 1) as ambiguous_dumps,
            minOrNullIf(
                greatest(b.observed_at, p.parent_observed_at),
                p.parent_count = 1
                and b.observed_at is not null
                and p.parent_observed_at is not null
            ) as first_valid_dump
        from blocks as b
        left join
            parents as p using (
                course_key, dump_id, section_position, subsection_position
            )
        where position(b.location, '+type@sequential+block@') = 0
        group by b.course_key, b.location
    )
select
    master_course,
    master_block_id,
    if(
        length(observed_parents) = 1 and incomplete_dumps = 0 and ambiguous_dumps = 0,
        observed_parents[1],
        null
    ) as subsection_block_id,
    multiIf(
        length(observed_parents) > 1 or ambiguous_dumps > 0,
        'ambiguous_lti_parent',
        incomplete_dumps > 0 or empty(observed_parents),
        'missing_lti_parent',
        'linked'
    ) as parent_status,
    if(parent_status = 'linked', first_valid_dump, null) as first_parent_observed_at
from observations
