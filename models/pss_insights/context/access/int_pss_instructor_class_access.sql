{{
    config(
        materialized="view",
        schema="pss_restricted",
        sql_security="definer",
        definer="CURRENT_USER",
        tags=["pss_context"],
    )
}}

with
    eligible_classes as (
        select institution_ccx_id, institution_id, ccx_id
        from {{ ref("int_pss_class_context") }}
        where
            class_scope_status = 'in_scope'
            and institution_status = 'active'
            and master_status in ('class_record', 'ccx_key', 'conflicting_master')
    ),

    profile_events as (
        select
            user_id,
            username,
            parseDateTime64BestEffortOrNull(time_last_dumped, 6, 'UTC') as dumped_at
        from {{ source("event_sink", "user_profile") }}
    ),

    -- A tied latest username or an undated observation might be current. Keep
    -- each candidate here so it also blocks another user claiming that login.
    current_logins as (
        select
            toString(user_id) as principal_user_id,
            username as principal_username,
            max(dumped_at) as last_seen,
            countIf(dumped_at is null) as undated_rows
        from profile_events
        group by user_id, username
        qualify
            last_seen = max(last_seen) over (partition by user_id) or undated_rows > 0
    ),

    verified_logins as (
        select principal_user_id, principal_username
        from current_logins
        qualify
            count() over (partition by principal_user_id) = 1
            -- Superset OAuth looks up stored users case-insensitively.
            and count() over (partition by lowerUTF8(principal_username)) = 1
            and undated_rows = 0
            and toInt32OrNull(principal_user_id) > 0
            and notEmpty(trimBoth(principal_username))
    ),

    role_grants as (
        select
            instructor.user_id as principal_user_id,
            instructor.username as principal_username,
            classes.institution_id as institution_id,
            classes.ccx_id as ccx_id,
            'instructor' as grant_role
        from {{ ref("int_course_licensing_instructor_class_current") }} as assignment
        inner join
            {{ ref("int_course_licensing_instructor_current") }} as instructor
            on assignment.instructor_id = instructor.source_id
            and assignment.user_id = instructor.user_id
        inner join
            {{ ref("int_course_licensing_instructor_institution_current") }}
            as affiliation
            on affiliation.instructor_id = assignment.instructor_id
            and affiliation.user_id = assignment.user_id
            and affiliation.institution_id = assignment.institution_id
        inner join
            eligible_classes as classes
            on classes.institution_ccx_id = assignment.institution_ccx_id
            and classes.institution_id = assignment.institution_id
            and classes.ccx_id = assignment.class_id
        where instructor.user_is_active = 'True' and affiliation.active = 'True'

        union all

        select
            admin.user_id as principal_user_id,
            admin.user_name as principal_username,
            classes.institution_id as institution_id,
            classes.ccx_id as ccx_id,
            'institution_admin' as grant_role
        from
            {{ ref("int_course_licensing_institution_administrator_current") }} as admin
        inner join
            eligible_classes as classes on admin.institution_id = classes.institution_id
        where admin.active = 'True'
    )
select
    grants.principal_user_id as principal_user_id,
    grants.principal_username as principal_username,
    grants.institution_id as institution_id,
    grants.ccx_id as ccx_id,
    max(grants.grant_role = 'instructor') as has_instructor_grant,
    max(grants.grant_role = 'institution_admin') as has_institution_admin_grant
from role_grants as grants
inner join
    verified_logins as logins
    on grants.principal_user_id = logins.principal_user_id
    and grants.principal_username = logins.principal_username
group by
    grants.principal_user_id,
    grants.principal_username,
    grants.institution_id,
    grants.ccx_id
