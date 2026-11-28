{{ config(tags=["pss_readiness"]) }}
-- Both omitted bounds explicitly attest a reconciled all-history membership.
-- A bounded row covers only the stated interval. All clocks and IDs are reviewed
-- inputs.
with
    evidence as (
        select
            institution_id,
            ccx_id,
            actor_id,
            snapshot_id,
            activity_type,
            coverage_start = '' and coverage_end = '' as full_history,
            parseDateTime64BestEffortOrNull(coverage_start, 6, 'UTC') as starts_at,
            parseDateTime64BestEffortOrNull(coverage_end, 6, 'UTC') as ends_at,
            parseDateTime64BestEffortOrNull(verified_at, 6, 'UTC') as checked_at,
            parseDateTime64BestEffortOrNull(valid_until, 6, 'UTC') as expires_at
        from
            {{
                ref(
                    var(
                        "pss_readiness_history_coverage_model",
                        "pss_readiness_history_coverage",
                    )
                )
            }}
        where
            notEmpty(trim(ifNull(evidence_ref, '')))
            and activity_type in ('Quiz', 'Lab', 'Practice Exam')
    )
select distinct *
from evidence
where
    checked_at <= now64(6, 'UTC')
    and expires_at > now64(6, 'UTC')
    and (full_history or (starts_at < ends_at and ends_at <= checked_at))
