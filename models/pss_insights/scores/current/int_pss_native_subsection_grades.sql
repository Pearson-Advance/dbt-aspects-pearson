{{ config(schema="pss_restricted", tags=["pss_scores"]) }}

-- This specific earned statement contains the LMS-calculated subsection totals.
-- The upstream grading_events projection defaults an absent scaled field to zero.
with
    observations as (
        select
            course_key as ccx_id,
            actor_id,
            toString(event_id) as event_id,
            toDateTime64(emission_time, 6, 'UTC') as emission_at,
            parseDateTime64BestEffortOrNull(
                JSONExtractString(event, 'timestamp'), 6, 'UTC'
            ) as statement_at,
            splitByString('/xblock/', object_id)[-1] as object_key,
            JSONExtractRaw(event, 'result', 'score', 'raw') as raw_value,
            JSONExtractRaw(event, 'result', 'score', 'max') as max_value,
            JSONExtractRaw(event, 'result', 'score', 'scaled') as scaled_value
        from {{ ref("xapi_events_all_parsed") }}
        where
            verb_id = 'http://id.tincanapi.com/verb/earned'
            and position(object_id, '/xblock/') > 0
            and position(object_id, '+type@sequential+block@') > 0
            and course_key in (select ccx_id from {{ ref("int_pss_class_scope") }})
    ),
    statements as (
        select
            ccx_id,
            event_id,
            min(emission_at) as first_emission_at,
            groupUniqArray(2)
            (
                tuple(
                    actor_id,
                    object_key,
                    statement_at,
                    raw_value,
                    max_value,
                    scaled_value
                )
            ) as variants,
            count() over (partition by event_id) as statement_classes
        from observations
        group by ccx_id, event_id
    ),
    events as (
        select
            ccx_id,
            event_id,
            first_emission_at,
            length(variants) = 1 and statement_classes = 1 as consistent,
            if(consistent, variants[1] .1, '') as actor_id,
            if(consistent, variants[1] .2, '') as object_key,
            if(consistent, variants[1] .3, null) as statement_at,
            coalesce(statement_at, first_emission_at) as graded_at,
            if(consistent, variants[1] .4, '') as raw_value,
            if(consistent, variants[1] .5, '') as max_value,
            if(consistent, variants[1] .6, '') as scaled_value
        from statements
    ),
    catalog as (
        select
            ccx_id,
            subsection_block_id,
            in_latest_publish,
            activity_type,
            arrayJoin([subsection_block_id, ccx_subsection_block_id]) as object_key
        from {{ ref("int_pss_activity_catalog") }}
    ),
    registered as (
        select ccx_id, actor_id
        from {{ ref("int_pss_learner_class_context") }}
        where is_registered = 1
    )
select
    e.ccx_id as ccx_id,
    nullIf(r.actor_id, '') as actor_id,
    nullIf(c.subsection_block_id, '') as subsection_block_id,
    e.event_id as event_id,
    e.graded_at as graded_at,
    e.statement_at as statement_at,
    e.first_emission_at as first_emission_at,
    toFloat64OrNull(e.raw_value) as earned_points,
    toFloat64OrNull(e.max_value) as possible_points,
    toFloat64OrNull(e.scaled_value) as scaled_score,
    multiIf(
        not e.consistent,
        'conflicting_statement',
        actor_id is null,
        'unregistered_or_unresolved_actor',
        subsection_block_id is null,
        'unmapped_subsection',
        not c.in_latest_publish,
        'historical_subsection',
        empty(ifNull(h.snapshot_id, ''))
        or (h.valid_until is not null and e.graded_at >= h.valid_until),
        'activity_type_at_grade_unavailable',
        h.activity_type != c.activity_type,
        'activity_type_changed',
        'resolved'
    ) as linkage_status,
    multiIf(
        not e.consistent,
        'conflicting_statement',
        e.raw_value in ('', 'null')
        or e.max_value in ('', 'null')
        or e.scaled_value in ('', 'null'),
        'missing_grade_field',
        earned_points is null
        or possible_points is null
        or scaled_score is null
        or not isFinite(earned_points)
        or not isFinite(possible_points)
        or not isFinite(scaled_score)
        or possible_points <= 0
        or earned_points < 0
        or earned_points > possible_points
        or scaled_score < 0
        or scaled_score > 1,
        'invalid_lms_score',
        abs(scaled_score - earned_points / nullIf(possible_points, 0)) > 0.000001,
        'inconsistent_lms_score',
        'observed'
    ) as score_status,
    if(
        score_status = 'observed',
        100.0 * (earned_points / nullIf(possible_points, 0)),
        null
    ) as score_pct
from events as e
left join catalog as c on e.ccx_id = c.ccx_id and e.object_key = c.object_key
left join registered as r on e.ccx_id = r.ccx_id and e.actor_id = r.actor_id
asof left join
    {{ ref("int_pss_activity_type_history") }} as h
    on e.ccx_id = h.ccx_id
    and c.subsection_block_id = h.subsection_block_id
    and e.graded_at >= h.valid_from
