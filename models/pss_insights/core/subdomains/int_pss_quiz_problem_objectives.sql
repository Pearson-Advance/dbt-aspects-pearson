{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(master_course, observed_at, problem_block_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [target.schema ~ ".int_pss_course_tag_captures_mv"],
            "depends_on_validation": true,
        },
        tags=["pss_insights", "pss_insights_subdomains"],
    )
}}

with
    -- A tag can change its parent or taxonomy. Resolve both child and parent
    -- against the dictionary that had been observed by this course export.
    tag_versions as (
        select
            toUInt64(id) as tag_id,
            assumeNotNull(parsed_at) as dictionary_at,
            any(taxonomy) as taxonomy_id,
            any(parent) as parent_id,
            any(value) as tag_name,
            any(external_id) as tag_external_id,
            any(lineage) as tag_lineage,
            max(undated_versions) = 0
            and uniqExact(tuple(taxonomy, parent, value, external_id, lineage))
            = 1 as valid
        from
            (
                select
                    *,
                    parseDateTime64BestEffortOrNull(
                        time_last_dumped, 6, 'UTC'
                    ) as parsed_at,
                    countIf(parsed_at is null) over (
                        partition by id
                    ) as undated_versions
                from {{ source("event_sink", "tag") }}
            )
        where parsed_at is not null and id > 0
        group by tag_id, dictionary_at
    ),
    taxonomy_first_seen as (
        select
            id,
            min(
                parseDateTime64BestEffortOrNull(time_last_dumped, 6, 'UTC')
            ) as first_seen
        from {{ source("event_sink", "taxonomy") }}
        group by id
    ),
    scope as (
        select
            s.master_course as master_course,
            groupUniqArray(s.taxonomy_id) as approved_ids,
            countIf(t.first_seen is null) = 0 as all_seen,
            max(t.first_seen) as scope_observed_at
        from {{ ref("int_pss_quiz_taxonomy_scope") }} as s
        left join taxonomy_first_seen as t on t.id = s.taxonomy_id
        group by s.master_course
    ),
    expanded as (
        select c.*, arrayJoin(emptyArrayToSingle(c.tag_ids)) as assigned_id
        from {{ ref("int_pss_course_tag_captures") }} as c
        where notEmpty(c.problem_block_id)
    ),
    resolved as (
        select
            c.master_course as master_course,
            c.observed_at as observed_at,
            c.problem_block_id as problem_block_id,
            c.capture_id as capture_id,
            c.subsection_block_id as subsection_block_id,
            c.is_latest_capture as is_latest_capture,
            c.capture_status as capture_status,
            s.all_seen and c.observed_at >= s.scope_observed_at as scope_available,
            c.assigned_id != 0 and (not t.valid or t.taxonomy_id <= 0) as tag_unknown,
            t.valid and has(s.approved_ids, t.taxonomy_id) as approved,
            t.parent_id = 0 as is_domain,
            p.valid
            and p.taxonomy_id = t.taxonomy_id
            and p.parent_id = 0 as parent_known,
            tuple(
                toUInt64(t.taxonomy_id),
                if(is_domain, t.tag_id, if(parent_known, p.tag_id, 0)),
                if(is_domain, toUInt64(0), t.tag_id),
                if(is_domain, t.tag_name, if(parent_known, p.tag_name, '')),
                if(is_domain, '', t.tag_name),
                if(
                    is_domain,
                    t.tag_external_id,
                    if(parent_known, p.tag_external_id, '')
                ),
                if(is_domain, '', t.tag_external_id),
                t.tag_lineage
            ) as objective
        from expanded as c
        left join scope as s on s.master_course = c.master_course
        asof left join
            tag_versions as t
            on t.tag_id = c.assigned_id
            and c.observed_at >= t.dictionary_at
        asof left join
            tag_versions as p
            on p.tag_id = toUInt64(t.parent_id)
            and c.observed_at >= p.dictionary_at
    ),
    grouped as (
        select
            master_course,
            observed_at,
            problem_block_id,
            capture_id,
            subsection_block_id,
            is_latest_capture,
            multiIf(
                capture_status != 'observed',
                capture_status,
                not ifNull(scope_available, false),
                'taxonomy_scope_unavailable',
                countIf(
                    tag_unknown or (approved and not is_domain and not parent_known)
                )
                > 0,
                'tag_dictionary_unavailable',
                'observed'
            ) as mapping_status,
            groupUniqArrayIf(
                objective, approved and ifNull(scope_available, false)
            ) as assignments
        from resolved
        group by
            master_course,
            observed_at,
            problem_block_id,
            capture_id,
            subsection_block_id,
            is_latest_capture,
            capture_status,
            scope_available
    )
select
    master_course,
    observed_at,
    problem_block_id,
    capture_id,
    subsection_block_id,
    is_latest_capture,
    mapping_status,
    arraySort(
        arrayFilter(
            x -> x .3 != 0
            or not arrayExists(
                y -> y .1 = x .1 and y .2 = x .2 and y .3 != 0, assignments
            ),
            assignments
        )
    ) as objectives,
    arrayMap(x -> tuple(x .1, x .2, x .3), objectives) as objective_keys,
    arrayMap(x -> tuple(x .1, x .2, x .3, x .4, x .5), objectives) as labels
from grouped settings join_use_nulls = 0
