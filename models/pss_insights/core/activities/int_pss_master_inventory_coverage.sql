{{ config(tags=["pss_insights", "pss_insights_activities"]) }}

with
    counts as (
        select
            master_course,
            count() as graded_count,
            countIf(coverage_status = 'classified') as classified_count
        from {{ ref("int_pss_activity_type_coverage") }}
        where in_latest_publish
        group by master_course
    )
select
    p.master_course as master_course,
    p.snapshot_id as snapshot_id,
    p.published_time as published_time,
    p.publication_status as publication_status,
    ifNull(c.graded_count, 0) as observed_graded_subsections,
    ifNull(c.classified_count, 0) as observed_classified_subsections,
    multiIf(
        p.publication_status != 'observed',
        p.publication_status,
        observed_graded_subsections = 0,
        'no_graded_subsections',
        observed_classified_subsections = 0,
        'graded_not_classified',
        observed_classified_subsections < observed_graded_subsections,
        'partially_classified',
        'classified'
    ) as inventory_status
from {{ ref("int_pss_master_publication") }} as p
left join counts as c on c.master_course = p.master_course
