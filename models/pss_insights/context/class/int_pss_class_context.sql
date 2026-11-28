{{ config(tags=["pss_context"]) }}

with
    class_keys as (
        select
            ccx_id,
            count() as source_row_count,
            if(count() = 1, any(nullIf(source_id, '')), null) as institution_ccx_id,
            if(
                count() = 1, any(nullIf(institution_id, '')), null
            ) as class_institution_id,
            if(count() = 1, any(nullIf(ccx_name, '')), null) as class_name,
            if(count() = 1, any(nullIf(master_course, '')), null) as assigned_master,
            if(count() = 1, any(nullIf(license_id, '')), null) as class_license_id,
            if(
                count() = 1, any(nullIf(license_status, '')), null
            ) as class_license_status
        from {{ ref("int_course_licensing_institution_ccx_current") }}
        where deleted = 'False' and notEmpty(trimBoth(ifNull(ccx_id, '')))
        group by ccx_id
    ),
    master_keys as (
        select
            *,
            nullIf(
                extract(
                    ccx_id,
                    '^ccx-v1:([\\p{L}\\p{N}_.:~-]+[+][\\p{L}\\p{N}_.:~-]+[+][\\p{L}\\p{N}_.:~-]+)[+]ccx@[0-9]+$'
                ),
                ''
            ) as locator_parts,
            concat('course-v1:', locator_parts) as locator_master,
            multiIf(
                source_row_count > 1,
                'duplicate_ccx',
                locator_master is null,
                'invalid_ccx_key',
                assigned_master = locator_master,
                'class_record',
                assigned_master is null,
                'ccx_key',
                'conflicting_master'
            ) as master_status,
            if(
                master_status in ('class_record', 'ccx_key'), locator_master, null
            ) as resolved_master
        from class_keys
    ),
    catalog as (
        select distinct master_course_id, toUInt8(1) as present
        from {{ ref("int_course_licensing_course_current") }}
    )
select
    c.ccx_id as ccx_id,
    c.institution_ccx_id as institution_ccx_id,
    c.class_institution_id as institution_id,
    c.class_name as class_name,
    c.resolved_master as master_course,
    c.assigned_master as assigned_master_course,
    c.master_status as master_status,
    coalesce(m.present, 0) as master_in_catalog,
    c.class_license_id as license_id,
    c.class_license_status as class_license_status,
    if(
        l.institution_id = c.class_institution_id, nullIf(l.status, ''), null
    ) as current_license_status,
    multiIf(
        c.class_license_id is null,
        null,
        empty(ifNull(l.source_id, '')),
        c.class_license_status,
        current_license_status
    ) as license_status,
    multiIf(
        c.source_row_count > 1,
        'duplicate_ccx',
        c.class_license_id is null,
        'no_license_id',
        empty(ifNull(l.source_id, '')),
        'missing_license',
        ifNull(l.institution_id = c.class_institution_id, 0) = 0,
        'conflicting_institution',
        empty(ifNull(l.status, '')),
        'missing_current_status',
        ifNull(l.status, '') != ifNull(c.class_license_status, ''),
        'status_mismatch',
        'matched'
    ) as license_record_status,
    multiIf(
        empty(ifNull(i.source_id, '')),
        'missing',
        i.active = 'True',
        'active',
        i.active = 'False',
        'inactive',
        'unknown'
    ) as institution_status,
    c.source_row_count as source_row_count,
    if(c.source_row_count > 1, 'duplicate_ccx', 'in_scope') as class_scope_status,
    d.class_start as class_start,
    d.class_end as class_end,
    {{
        pss_context_active_on(
            "d.class_start",
            "d.class_end",
            "toDate(now(), '" ~ var("pss_reporting_timezone", "UTC") ~ "')",
        )
    }} as is_active_today,
    coalesce(
        nullIf(d.class_date_status, ''), 'missing_ccx_overview'
    ) as class_date_status,
    nullIf(d.overview_dumped_at, '') as overview_dumped_at
from master_keys as c
left join
    {{ ref("int_course_licensing_license_current") }} as l
    on c.class_license_id = l.source_id
left join
    {{ ref("int_course_licensing_institution_current") }} as i
    on c.class_institution_id = i.source_id
left join catalog as m on c.resolved_master = m.master_course_id
left join {{ ref("int_pss_ccx_overview_dates") }} as d on c.ccx_id = d.ccx_id
