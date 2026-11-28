-- Independent reconciliation certifies a bounded interval across all applicable
-- providers, registered actors, linkage and participation rules for this activity.
with
    certificates as (
        select distinct
            ccx_id,
            subsection_block_id,
            snapshot_id,
            parseDateTime64BestEffortOrNull(coverage_start, 6, 'UTC') as starts_at,
            parseDateTime64BestEffortOrNull(coverage_end, 6, 'UTC') as ends_at,
            parseDateTime64BestEffortOrNull(verified_at, 6, 'UTC') as checked_at
        from
            {{
                ref(
                    var(
                        "pss_attempt_coverage_evidence_model",
                        "pss_attempt_coverage_evidence",
                    )
                )
            }}
        where notEmpty(trim(ifNull(evidence_ref, '')))
    ),
    eligible as (
        select distinct
            c.institution_id as institution_id,
            c.ccx_id as ccx_id,
            c.subsection_block_id as subsection_block_id,
            c.snapshot_id as snapshot_id,
            assumeNotNull(e.starts_at) as window_start,
            assumeNotNull(e.ends_at) as window_end
        from certificates as e
        inner join
            {{ ref("int_pss_activity_catalog") }} as c
            on e.ccx_id = c.ccx_id
            and e.subsection_block_id = c.subsection_block_id
            and e.snapshot_id = c.snapshot_id
        inner join {{ ref("int_pss_roster_coverage") }} as r on r.ccx_id = c.ccx_id
        inner join
            {{ ref("int_pss_class_inventory_coverage") }} as i on i.ccx_id = c.ccx_id
        where
            c.in_latest_publish
            and i.inventory_status = 'classified'
            and r.registered_actor_denominator is not null
            and e.starts_at < e.ends_at
            and e.ends_at <= e.checked_at
            and e.checked_at <= now64(6, 'UTC')
    ),
    boundaries as (
        select
            *,
            maxOrNull(window_end) over (
                partition by ccx_id, subsection_block_id
                order by window_start, window_end
                rows between unbounded preceding and 1 preceding
            ) as prior_end
        from eligible
    ),
    islands as (
        select
            *,
            sum(prior_end is null or window_start > prior_end) over (
                partition by ccx_id, subsection_block_id
                order by window_start, window_end
                rows unbounded preceding
            ) as interval_group
        from boundaries
    )
select
    institution_id,
    ccx_id,
    subsection_block_id,
    snapshot_id,
    min(window_start) as coverage_start,
    max(window_end) as coverage_end
from islands
group by institution_id, ccx_id, subsection_block_id, snapshot_id, interval_group
