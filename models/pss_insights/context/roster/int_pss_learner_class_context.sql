{{ config(tags=["pss_context"]) }}

select
    ccx_id,
    assumeNotNull(actor_id) as actor_id,
    is_registered,
    enrollment_status,
    enrollment_states,
    length(source_enrollment_keys) as source_enrollment_count
from {{ ref("int_pss_roster_evidence") }} as evidence
where evidence.identity_status = 'resolved'
