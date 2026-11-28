{{ config(schema="pss_restricted", tags=["pss_context"]) }}

with
    -- ExternalId is unique on (LMS user, type) and on UUID in Django. The
    -- event sink can contain repeated dumps or broken identities. Check both
    -- directions across all users before restricting to this roster.
    xapi_pairs as (
        select distinct
            toString(user_id) as lms_user_id, toString(external_user_id) as xapi_uuid
        from {{ source("event_sink", "external_id") }}
        where
            external_id_type = 'xapi'
            and user_id > 0
            and toUUIDOrZero(toString(external_user_id))
            != toUUID('00000000-0000-0000-0000-000000000000')
    ),
    actor_owners as (
        select *, count() over (partition by xapi_uuid) as owner_count from xapi_pairs
    ),
    identities as (
        select
            lms_user_id,
            min(xapi_uuid) as candidate_actor,
            count() as uuid_count,
            max(owner_count) as max_owner_count
        from actor_owners
        group by lms_user_id
    ),
    scoped as (
        select
            c.ccx_id as ccx_id,
            e.enrollment_key as enrollment_key,
            e.enrollment_source as enrollment_source,
            e.enrollment_status as enrollment_status,
            nullIf(ifNull(e.user_id, ''), '') as source_user,
            ifNull(
                c.ccx_id = e.class_id
                and c.institution_ccx_id = e.institution_ccx_id
                and c.institution_id = e.institution_id,
                false
            ) as scope_matches
        from {{ ref("int_course_licensing_enrollment_current") }} as e
        inner join
            {{ ref("int_pss_class_context") }} as c
            on e.class_id = c.ccx_id
            or e.institution_ccx_id = c.institution_ccx_id
    ),
    registrations as (
        select
            ccx_id,
            -- Invalid mappings and invitations retain only their enrollment
            -- keys. Never associate their learner or actor with either class.
            if(
                scope_matches
                and enrollment_source = 'licensed_enrollment'
                and source_user is not null,
                concat('user:', assumeNotNull(source_user)),
                enrollment_key
            ) as record_key,
            if(startsWith(record_key, 'user:'), min(source_user), null) as lms_user_id,
            min(scope_matches) as has_matching_scope,
            arraySort(groupUniqArray(enrollment_key)) as source_enrollment_keys,
            arraySort(groupUniqArray(enrollment_status)) as enrollment_states
        from scoped
        group by ccx_id, record_key
    ),
    classified as (
        select
            r.ccx_id as ccx_id,
            r.record_key as record_key,
            r.lms_user_id as lms_user_id,
            r.source_enrollment_keys as source_enrollment_keys,
            r.enrollment_states as enrollment_states,
            if(
                length(r.enrollment_states) = 1, r.enrollment_states[1], 'Conflicting'
            ) as enrollment_status,
            multiIf(
                not r.has_matching_scope,
                'class_key_mismatch',
                enrollment_status = 'Pending',
                'invitation',
                r.lms_user_id is null,
                'missing_lms_user',
                ifNull(i.uuid_count, 0) = 0,
                'no_xapi_uuid',
                i.uuid_count > 1,
                'multiple_xapi_uuids',
                i.max_owner_count > 1,
                'shared_xapi_uuid',
                'resolved'
            ) as identity_status,
            if(identity_status = 'resolved', i.candidate_actor, null) as actor_id,
            multiIf(
                not r.has_matching_scope,
                null,
                enrollment_status = 'Pending',
                toUInt8(0),
                enrollment_status = 'Active',
                toUInt8(1),
                null
            ) as is_registered
        from registrations as r
        left join identities as i on r.lms_user_id = i.lms_user_id
    )
select *
from classified
