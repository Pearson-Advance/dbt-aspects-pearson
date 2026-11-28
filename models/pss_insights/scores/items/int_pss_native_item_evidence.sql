{{
    config(
        materialized="materialized_view",
        schema="pss_restricted",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(ccx_id, source_result_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [target.schema ~ ".dim_course_blocks_mv"],
        },
        tags=["pss_scores", "pss_items"],
    )
}}

-- Each evaluated statement describes a problem or one of its child answers.
-- The attempt-id extension is the problem's own ordinal. Neither identifies
-- a whole-subsection sitting. Retain the smallest auditable native evidence
-- without exporting the answer, original actor IRI, or full xAPI document.
with
    observations as (
        select
            course_key as ccx_id,
            toString(event_id) as source_result_id,
            actor_id,
            extract(object_id, '/xblock/([^/?#]+)') as object_usage_key,
            extract(
                JSON_VALUE(event, '$.context.contextActivities.parent[0].id'),
                '/xblock/([^/?#]+)'
            ) as parent_usage_key,
            parseDateTime64BestEffortOrNull(
                JSON_VALUE(event, '$.timestamp'), 6, 'UTC'
            ) as statement_at,
            toTimeZone(emission_time, 'UTC') as emission_at,
            JSONExtractRaw(event, 'result', 'score', 'raw') as raw_points,
            JSONExtractRaw(event, 'result', 'score', 'max') as max_points,
            JSONExtractRaw(event, 'result', 'success') as success,
            JSON_VALUE(
                event,
                '$.object.definition.extensions."http://id.tincanapi.com/extension/attempt-id"'
            ) as problem_attempt_ordinal
        from {{ ref("xapi_events_all_parsed") }}
        where
            verb_id = 'https://w3id.org/xapi/acrossx/verbs/evaluated'
            and course_key in (select ccx_id from {{ ref("int_pss_class_scope") }})
    ),
    statements as (
        select
            ccx_id,
            source_result_id,
            min(emission_at) as first_emission_at,
            groupUniqArray(2)
            (
                tuple(
                    actor_id,
                    object_usage_key,
                    parent_usage_key,
                    statement_at,
                    raw_points,
                    max_points,
                    success,
                    problem_attempt_ordinal
                )
            ) as variants
        from observations
        group by ccx_id, source_result_id
    ),
    items as (
        select
            ccx_id,
            source_result_id,
            first_emission_at,
            length(variants) = 1 as consistent,
            if(consistent, variants[1] .1, '') as observed_actor,
            if(consistent, variants[1] .2, '') as object_usage_key,
            if(consistent, variants[1] .3, '') as parent_usage_key,
            if(consistent, variants[1] .4, null) as statement_at,
            if(consistent, toFloat64OrNull(variants[1] .5), null) as raw_points,
            if(consistent, toFloat64OrNull(variants[1] .6), null) as max_points,
            if(consistent, variants[1] .7, '') as success,
            if(
                consistent, toUInt32OrNull(variants[1] .8), null
            ) as problem_attempt_ordinal
        from statements
    ),
    candidate_problems as (
        select
            a.ccx_id as ccx_id,
            a.subsection_block_id as subsection_block_id,
            a.activity_type as activity_type,
            b.block_id as problem_block_id,
            arrayJoin(
                [b.block_id, {{ pss_ccx_block_key("b.block_id", "s.ccx_number") }}]
            ) as usage_key
        from {{ ref("int_pss_activity_catalog") }} as a
        inner join {{ ref("int_pss_class_scope") }} as s on a.ccx_id = s.ccx_id
        inner join
            {{ ref("dim_course_blocks") }} as b
            on a.master_course = b.course_key
            and a.subsection_block_id = b.subsection_block_id
        where a.in_latest_publish and b.in_latest_publish and b.block_type = 'problem'
    ),
    current_problems as (
        select
            ccx_id,
            usage_key,
            uniqExact(
                tuple(problem_block_id, subsection_block_id, activity_type)
            ) as mappings,
            if(mappings = 1, min(problem_block_id), null) as resolved_problem,
            if(mappings = 1, min(subsection_block_id), null) as resolved_subsection,
            if(mappings = 1, min(activity_type), null) as resolved_type
        from candidate_problems
        group by ccx_id, usage_key
    )
select
    i.ccx_id as ccx_id,
    i.source_result_id as source_result_id,
    nullIf(r.actor_id, '') as actor_id,
    nullIf(i.object_usage_key, '') as source_item_id,
    nullIf(p.resolved_problem, '') as problem_block_id,
    nullIf(p.resolved_subsection, '') as current_subsection_block_id,
    nullIf(p.resolved_type, '') as current_activity_type,
    i.first_emission_at as first_emission_at,
    i.statement_at as statement_at,
    i.problem_attempt_ordinal as problem_attempt_ordinal,
    if(isFinite(i.raw_points), i.raw_points, null) as earned_points,
    if(isFinite(i.max_points), i.max_points, null) as possible_points,
    multiIf(
        i.success = 'true', toUInt8(1), i.success = 'false', toUInt8(0), null
    ) as succeeded,
    multiIf(
        not i.consistent,
        'conflicting_statement',
        empty(ifNull(r.actor_id, '')),
        'registered_actor_unresolved',
        p.mappings > 1,
        'ambiguous_current_activity',
        empty(ifNull(p.resolved_problem, '')),
        'current_activity_unresolved',
        'whole_sitting_key_unavailable'
    ) as linkage_status
from items as i
left join
    {{ ref("int_pss_learner_class_context") }} as r
    on i.ccx_id = r.ccx_id
    and i.observed_actor = r.actor_id
    and r.is_registered = 1
left join
    current_problems as p
    on i.ccx_id = p.ccx_id
    and if(
        position(i.parent_usage_key, '+type@problem+block@') > 0,
        i.parent_usage_key,
        i.object_usage_key
    )
    = p.usage_key
