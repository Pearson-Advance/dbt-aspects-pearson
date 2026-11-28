-- Bounded windows use the merged, independently reconciled score model ledger.
select
    institution_id,
    ccx_id,
    subsection_block_id,
    snapshot_id,
    toNullable(coverage_start) as coverage_start,
    toNullable(coverage_end) as coverage_end,
    false as full_history,
    coverage_end as verified_through
from {{ ref("int_pss_activity_attempt_coverage") }}
union all
select
    institution_id,
    ccx_id,
    subsection_block_id,
    snapshot_id,
    cast(null as Nullable(DateTime64(6, 'UTC'))) as coverage_start,
    cast(null as Nullable(DateTime64(6, 'UTC'))) as coverage_end,
    true as full_history,
    checked_at as verified_through
from
    (
        {{
            pss_valid_reporting_attestations(
                ref(
                    var(
                        "pss_activity_coverage_model", "pss_activity_history_coverage"
                    )
                )
            )
        }}
    )
where notEmpty(ifNull(subsection_block_id, ''))
