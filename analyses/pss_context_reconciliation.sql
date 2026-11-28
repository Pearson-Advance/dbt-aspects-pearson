{# Analyst-only reconciliation. Supply an independent LMS export in the restricted
   database. See models/pss_insights/context/README.md for the export contract. #}
with
    actual as (
        select
            'class' as record_type,
            ccx_id,
            '' as subject_key,
            [
                institution_ccx_id,
                institution_id,
                master_course,
                license_id,
                license_status,
                toString(class_start),
                toString(class_end)
            ] as attributes
        from {{ ref("int_pss_class_context") }}

        union all

        select
            'membership', ccx_id, actor_id, [enrollment_status, toString(is_registered)]
        from {{ ref("int_pss_learner_class_context") }}

        union all

        select
            'roster',
            ccx_id,
            '',
            [
                toString(registered_source_learners),
                toString(resolved_registered_learners),
                toString(unresolved_registered_learners),
                toString(pending_invitations)
            ]
        from {{ ref("int_pss_roster_coverage") }}

        union all

        select
            'grant',
            ccx_id,
            principal_user_id,
            [
                institution_id,
                principal_username,
                toString(has_instructor_grant),
                toString(has_institution_admin_grant)
            ]
        from {{ ref("int_pss_instructor_class_access") }}
    ),
    comparisons as (
        select record_type, ccx_id, subject_key, attributes, 0 as from_lms
        from actual
        union all
        select record_type, ccx_id, subject_key, attributes, 1 as from_lms
        from
            {{ ref("int_pss_roster_evidence").include(identifier=false) }}.pss_lms_context_at_cutoff
    ),
    differences as (
        select
            record_type,
            ccx_id,
            subject_key,
            arraySort(groupArrayIf(attributes, from_lms = 1)) as lms_values,
            arraySort(groupArrayIf(attributes, from_lms = 0)) as reporting_values
        from comparisons
        group by record_type, ccx_id, subject_key
    )
select
    *,
    multiIf(
        empty(lms_values),
        'only_in_reporting',
        empty(reporting_values),
        'only_in_lms',
        'different_values_or_row_count'
    ) as discrepancy
from differences
where lms_values != reporting_values
order by record_type, ccx_id, subject_key
