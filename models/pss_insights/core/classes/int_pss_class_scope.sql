{{ config(tags=["pss_insights", "pss_insights_classes"]) }}

select
    institution_ccx_id,
    institution_id,
    license_id,
    license_status,
    ccx_id,
    class_name,
    master_course,
    master_status = 'ccx_key' as master_course_from_ccx_key,
    splitByString('+ccx@', ccx_id)[2] as ccx_number
from {{ ref("int_pss_class_context") }}
where class_scope_status = 'in_scope'
