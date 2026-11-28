{{ config(schema="pss_restricted") }}

-- Reconcile against the exact institution and publication. A known unlinked
-- sitting can only change the final outcome of the learner who took it.
with
    attested as (
        select
            institution_id,
            ccx_id,
            snapshot_id,
            min(final_attempts_complete = 1) as finals_checked,
            min(navigation_complete = 1) as navigation_checked
        from
            (
                {{
                    pss_valid_reporting_attestations(
                        ref(
                            var(
                                "pss_class_coverage_model",
                                "pss_reporting_class_coverage_evidence",
                            )
                        )
                    )
                }}
            )
        group by institution_id, ccx_id, snapshot_id
    ),
    gaps as (
        select institution_id, ccx_id, groupUniqArray(actor_id) as actors
        from {{ ref("int_pss_reporting_unlinked_attempts") }}
        where activity_type is null or activity_type = 'Final Exam'
        group by institution_id, ccx_id
    )
select
    c.institution_id as institution_id,
    c.ccx_id as ccx_id,
    c.snapshot_id as snapshot_id,
    c.catalog_status = 'complete' and e.finals_checked as final_attempts_complete,
    g.actors as unresolved_final_actors,
    e.navigation_checked as navigation_complete
from {{ ref("int_pss_class_catalog_coverage") }} as c
inner join
    attested as e
    on c.institution_id = e.institution_id
    and c.ccx_id = e.ccx_id
    and c.snapshot_id = e.snapshot_id
left join gaps as g on c.institution_id = g.institution_id and c.ccx_id = g.ccx_id
