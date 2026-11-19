{{ config(tags=["pss_insights", "pss_reporting", "pss_reporting_fixture"]) }}

with
    access_scope as (
        select *
        from
        values
            (
                'institution_id String, ccx_id String',
                ('I', 'A'),
                ('I', 'A'),
                ('I', 'B'),
                ('I', 'C')
            )
    ),
    classes as (
        select *
        from
        values
            (
                'institution_id String, ccx_id String, master_course String, catalog_status String, known_published_activities UInt64, registered_learners UInt64',
                ('I', 'A', 'M', 'complete', 4, 3),
                ('I', 'B', 'M', 'incomplete', 1, 2),
                ('I', 'C', 'M', 'complete', 0, 0),
                ('other', 'X', 'M', 'complete', 0, 1)
            )
    ),
    delivery as (
        select *
        from
        values
            (
                'institution_id String, ccx_id String, master_course String, activity_type String, delivery_complete Nullable(UInt8)',
                ('I', 'A', 'M', 'Quiz', 1),
                ('I', 'A', 'M', 'Quiz', 1),
                ('I', 'A', 'M', 'Quiz', 0),
                ('I', 'A', 'M', 'Final Exam', null),
                ('I', 'B', 'M', 'Quiz', 1),
                ('other', 'X', 'M', 'Lab', 1)
            )
    ),
    actual as ({{ pss_delivery_type_counts("access_scope", "classes", "delivery") }})
select ccx_id
from actual
group by ccx_id
having
    count() != 4
    or uniqExact(activity_type) != 4
    or ccx_id not in ('A', 'B', 'C')
    or (
        ccx_id = 'A'
        and (
            countIf(
                activity_type = 'Quiz'
                and total_activities = 3
                and completed_activities = 2
                and remaining_activities = 1
            )
            != 1
            or countIf(
                activity_type in ('Lab', 'Practice Exam')
                and total_activities = 0
                and remaining_activities = 0
            )
            != 2
            or countIf(
                activity_type = 'Final Exam'
                and total_activities = 1
                and remaining_activities is null
            )
            != 1
        )
    )
    or (
        ccx_id = 'B'
        and (
            sum(known_activities) != 1
            or count(total_activities) != 0
            or count(completed_activities) != 0
        )
    )
    or (
        ccx_id = 'C'
        and (countIf(total_activities = 0) != 4 or count(remaining_activities) != 0)
    )
union all
select 'missing class'
where (select uniqExact(ccx_id) from actual) != 3
