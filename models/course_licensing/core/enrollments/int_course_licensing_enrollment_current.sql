{{
    config(
        tags=[
            "course_licensing",
            "course_licensing_enrollments",
            "course_licensing_current",
        ],
    )
}}

{#
    Business-level union of every current Course Licensing enrollment,
    licensed and pending alike. licensed_enrollment and course_enrollment_allowed
    each have their own independent source_id sequence (separate Django
    tables), so enrollment_key prefixes each with its origin to avoid
    collisions.

    course_enrollment_allowed carries no FK to institution_ccx — it is a
    platform-owned Open edX model shared by regular course invites and
    licensed-class invites alike (see the source column's own
    documentation). A pending row only belongs here when its course_id
    resolves to a real Course Licensing class (inner join to
    institution_ccx on course_id = ccx_id) and it has not yet been claimed
    by a registered user (user_id is null) — the same opportunistic join
    Pearson's own license-enrollment counting logic uses. Every other
    course_enrollment_allowed row is an ordinary course enrollment with
    nothing to do with Course Licensing, and is correctly excluded by the
    inner join.
#}
with
    licensed_enrollments as (
        select
            concat('licensed_enrollment:', toString(source_id)) as enrollment_key,
            'licensed_enrollment' as enrollment_source,
            source_id,
            institution_id,
            institution_ccx_id,
            license_id,
            user_id,
            student_email as learner_email,
            class_id,
            class_name,
            case
                when expired = 'True'
                then 'Expired'
                when is_active = 'True'
                then 'Active'
                else 'Inactive'
            end as enrollment_status,
            (is_active = 'True' or expired = 'True') as consumes_seat,
            enrollment_created as created,
            end_date
        from {{ ref("int_course_licensing_licensed_enrollment_current") }}
    ),

    pending_enrollments as (
        select
            concat(
                'course_enrollment_allowed:', toString(enrollment.source_id)
            ) as enrollment_key,
            'course_enrollment_allowed' as enrollment_source,
            enrollment.source_id,
            class.institution_id,
            class.source_id as institution_ccx_id,
            class.license_id,
            enrollment.user_id,
            enrollment.email as learner_email,
            class.ccx_id as class_id,
            class.ccx_name as class_name,
            'Pending' as enrollment_status,
            true as consumes_seat,
            enrollment.created,
            cast(null as Nullable(String)) as end_date
        from
            {{ ref("int_course_licensing_course_enrollment_allowed_current") }}
            as enrollment
        inner join
            {{ ref("int_course_licensing_institution_ccx_current") }} as class
            on enrollment.course_id = class.ccx_id
        where enrollment.user_id is null
    )

select *
from licensed_enrollments
union all
select *
from pending_enrollments
