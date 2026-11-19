{{ config(tags=["pss_insights", "pss_insights_classes"]) }}

{#
    Single entry point for the PSS class population: if a richer class-context
    model is introduced, repoint only this model.

    Tombstones are already removed by the _current model; deleted = 'False'
    drops classes soft-deleted by the application. No license-status filter:
    license_status is exposed so consumers can filter.

    master_course falls back to the course key embedded in the CCX key when
    the sink left it empty; master_course_from_ccx_key flags those rows.
#}
select
    ccx.source_id as institution_ccx_id,
    ccx.institution_id as institution_id,
    ccx.license_id as license_id,
    ccx.license_status as license_status,
    ccx.ccx_id as ccx_id,
    ccx.ccx_name as class_name,
    coalesce(
        nullIf(ccx.master_course, ''), {{ pss_course_key_from_ccx_key("ccx.ccx_id") }}
    ) as master_course,
    coalesce(ccx.master_course, '') = '' as master_course_from_ccx_key,
    splitByString('+ccx@', ccx.ccx_id)[2] as ccx_number
from {{ ref("int_course_licensing_institution_ccx_current") }} as ccx
where ccx.deleted = 'False'
