{{ config(tags=["pss_context"]) }}

{% set expiry = var("pss_roster_reconciled_until_utc", "") | string %}
{% if not modules.re.fullmatch(
    "[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}", expiry
) %}
    {% set expiry = "" %}
{% endif %}

with
    toDateTimeOrNull('{{ expiry }}', 'UTC') as attestation_expiry,
    per_class as (
        select
            ccx_id,
            countIf(
                is_registered = 1 and lms_user_id is not null
            ) as registered_source_learners,
            countIf(
                is_registered = 1 and identity_status = 'resolved'
            ) as resolved_registered_learners,
            countIf(identity_status = 'invitation') as pending_invitations,
            countIf(identity_status = 'missing_lms_user') as missing_user_records,
            countIf(identity_status = 'class_key_mismatch') as scope_mismatch_records,
            countIf(
                is_registered is null and identity_status != 'class_key_mismatch'
            ) as registration_policy_pending,
            countIf(lms_user_id is not null) as licensed_source_learners,
            countIf(
                lms_user_id is not null and has(enrollment_states, 'Active')
            ) as learners_with_active_state,
            countIf(
                lms_user_id is not null and has(enrollment_states, 'Inactive')
            ) as learners_with_inactive_state,
            countIf(
                lms_user_id is not null and has(enrollment_states, 'Expired')
            ) as learners_with_expired_state
        from {{ ref("int_pss_roster_evidence") }}
        group by ccx_id
    ),
    counts as (
        select
            c.ccx_id as ccx_id,
            c.institution_ccx_id is not null
            and c.institution_id is not null as scope_known,
            ifNull(p.registered_source_learners, 0) as registered_source_learners,
            ifNull(p.resolved_registered_learners, 0) as resolved_registered_learners,
            toUInt64(
                registered_source_learners - resolved_registered_learners
            ) as unresolved_registered_learners,
            ifNull(p.pending_invitations, 0) as pending_invitations,
            ifNull(p.missing_user_records, 0) as missing_user_records,
            ifNull(p.scope_mismatch_records, 0) as scope_mismatch_records,
            ifNull(p.registration_policy_pending, 0) as registration_policy_pending,
            ifNull(p.licensed_source_learners, 0) as licensed_source_learners,
            ifNull(p.learners_with_active_state, 0) as learners_with_active_state,
            ifNull(p.learners_with_inactive_state, 0) as learners_with_inactive_state,
            ifNull(p.learners_with_expired_state, 0) as learners_with_expired_state
        from {{ ref("int_pss_class_context") }} as c
        left join per_class as p on c.ccx_id = p.ccx_id
    )
select
    * except (scope_known),
    multiIf(
        not scope_known,
        'class_scope_unavailable',
        scope_mismatch_records > 0,
        'class_key_mismatch',
        missing_user_records > 0 or unresolved_registered_learners > 0,
        'identity_incomplete',
        registration_policy_pending > 0,
        'registration_policy_pending',
        registered_source_learners > 0,
        'resolved',
        pending_invitations > 0,
        'pending_only',
        'empty'
    ) as roster_status,
    ifNull(
        toString(attestation_expiry, 'UTC') = '{{ expiry }}'
        and now('UTC') < attestation_expiry,
        false
    ) as source_reconciled,
    if(
        source_reconciled = 1
        and roster_status in ('resolved', 'pending_only', 'empty'),
        resolved_registered_learners,
        null
    ) as registered_actor_denominator
from counts
