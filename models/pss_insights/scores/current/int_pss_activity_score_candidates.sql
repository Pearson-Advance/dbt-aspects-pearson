{{ config(tags=["pss_scores"]) }}

-- Keep every tied latest LMS event. Arbitrarily choosing a statement UUID
-- would turn disagreeing grades into a definitive result.
with
    native as (
        select
            ccx_id,
            assumeNotNull(actor_id) as actor_id,
            assumeNotNull(subsection_block_id) as subsection_block_id,
            'native' as provider,
            event_id as source_id,
            graded_at as observed_at,
            true as is_finished,
            if(linkage_status = 'resolved', score_pct, null) as score_pct,
            if(
                linkage_status = 'resolved', score_status, linkage_status
            ) as score_status
        from {{ ref("int_pss_native_subsection_grades") }}
        where linkage_status in ('resolved', 'activity_type_at_grade_unavailable')
        qualify
            graded_at
            = max(graded_at) over (partition by ccx_id, actor_id, subsection_block_id)
    ),
    provider as (
        select
            ccx_id,
            actor_id,
            subsection_block_id,
            provider,
            attempt_key as source_id,
            attempt_finished_at as observed_at,
            is_finished,
            if(activity_type is null, null, score_pct) as score_pct,
            multiIf(
                activity_type is null,
                'activity_type_at_start_unavailable',
                not is_finished,
                'unfinished_attempt',
                score_pct is null,
                score_status,
                'observed'
            ) as score_status
        from {{ ref("rpt_pss_activity_attempts") }}
        where
            provider = 'measureup'
            and (activity_type is null or activity_type = catalog_activity_type)
    )
select *
from native
union all
select *
from provider
