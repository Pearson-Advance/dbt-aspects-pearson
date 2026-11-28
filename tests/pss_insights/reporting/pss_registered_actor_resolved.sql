{{ config(tags=["pss_reporting"]) }}

-- A reconciled denominator must never silently exclude an unresolved learner.
select ccx_id
from {{ ref("int_pss_roster_coverage") }}
where
    registered_actor_denominator is not null
    and (
        unresolved_registered_learners > 0
        or resolved_registered_learners != registered_actor_denominator
        or not source_reconciled
    )
