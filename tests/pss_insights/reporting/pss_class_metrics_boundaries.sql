{{ config(tags=["pss_insights", "pss_reporting", "pss_reporting_fixture"]) }}

-- Keys 68 through 81 have 100 learners with available readiness and one
-- without readiness. Classes 2 and 3 exercise missing readiness and empty
-- rosters. Class 1 has a complete, genuinely empty inventory.
with
    classes as (
        select
            toString(arrayJoin([1, 2, 3, 68, 69, 70, 71, 79, 80, 81])) as ccx_id,
            'institution' as institution_id,
            ccx_id as institution_ccx_id,
            'master' as master_course,
            ccx_id as class_name,
            'license' as license_id,
            'active' as license_status,
            if(ccx_id = '3', null, toDate('2026-01-01')) as class_start,
            if(ccx_id = '81', toDate('2025-12-31'), toDate('2026-01-01')) as class_end
    ),
    learners as (
        select
            c.ccx_id as ccx_id,
            c.institution_id as institution_id,
            c.master_course as master_course,
            toString(n.number) as actor_id,
            multiIf(
                c.ccx_id = '2' or n.number = 100,
                null,
                n.number < toUInt8(c.ccx_id),
                'On Target',
                n.number % 2 = 0,
                'Borderline',
                'At Risk'
            ) as readiness_status,
            toNullable(0.85) as course_grade,
            toNullable(0.5) as content_progress,
            if(c.ccx_id = '68', 0.70, 0.69) as quiz_score_sum,
            1 as quiz_score_count,
            0 as lab_score_sum,
            0 as lab_score_count,
            if(c.ccx_id = '68', 0.70, 0.5) as practice_score_sum,
            1 as practice_score_count,
            0.7 as final_score_sum,
            1 as final_score_count,
            toUInt8(n.number < 2) as final_attempted,
            if(n.number < 2, toUInt8(n.number = 0), null) as final_passed
        from classes as c
        cross join numbers(101) as n
        where c.ccx_id != '3' and (c.ccx_id not in ('1', '2') or n.number = 0)
    ),
    practice as (
        -- For A the item-weighted score is 1/10. Its unweighted mean would
        -- be 1/2. B scores 1/4. The non-roster actor cannot change the rank.
        select *
        from
        values
            (
                'ccx_id String, actor_id String, domain_id String, subdomain_id String, subdomain_name String, correct_items UInt64, total_items UInt64',
                ('69', '0', 'D', 'A', 'Routing', 1, 1),
                ('69', '1', 'D', 'A', 'Routing', 0, 9),
                ('69', '0', 'D', 'B', 'Security', 1, 4),
                ('69', 'visitor', 'D', 'A', 'Routing', 99, 99)
            )
    ),
    catalogs as (
        select
            c.ccx_id as ccx_id,
            c.institution_id as institution_id,
            c.master_course as master_course,
            if(c.ccx_id = '79', 'incomplete', 'complete') as catalog_status,
            toUInt64(c.ccx_id != '1') as known_published_activities
        from classes as c
    ),
    delivered as (
        select
            c.ccx_id as ccx_id,
            c.institution_id as institution_id,
            c.master_course as master_course,
            if(
                c.ccx_id in ('71', '3'), null, toUInt8(c.ccx_id = '69')
            ) as delivery_complete
        from classes as c
        where c.ccx_id != '1'
    ),
    actual as (
        {{
            pss_class_metrics(
                "classes",
                "learners",
                "practice",
                "catalogs",
                "delivered",
                "toDate('2026-01-01')",
            )
        }}
    ),
    expected as (
        select *
        from
        values
            (
                'ccx_id String, risk String, attention Int8, support Int8, reasons String, active Int8, registered UInt64, unknown UInt64, total Int8, complete Int8, remaining Int8',
                ('1', 'Low', 0, 0, '', 1, 1, 0, 0, 0, 0),
                ('2', 'NA', -1, -1, '', 1, 1, 1, 1, 0, 1),
                ('3', 'NA', -1, -1, '', -1, 0, 0, 1, -1, -1),
                ('68', 'High', 1, 0, '', 1, 101, 1, 1, 0, 1),
                ('69', 'High', 1, 1, 'Quiz,Practice Exam', 1, 101, 1, 1, 1, 0),
                ('70', 'Medium', 1, 0, '', 1, 101, 1, 1, 0, 1),
                ('71', 'Medium', 1, 0, '', 1, 101, 1, 1, -1, -1),
                ('79', 'Medium', 1, 0, '', 1, 101, 1, -1, -1, -1),
                ('80', 'Low', 0, 0, '', 1, 101, 1, 1, 0, 1),
                ('81', 'Low', 0, 0, '', 0, 101, 1, 1, 0, 1)
            )
    )
select e.ccx_id
from expected as e
left join actual as a on e.ccx_id = a.ccx_id
where
    coalesce(a.ccx_id, '') != e.ccx_id
    or coalesce(a.class_risk, 'NA') != e.risk
    or coalesce(toInt16(a.requires_attention), -1) != e.attention
    or coalesce(toInt16(a.requires_instructor_support), -1) != e.support
    or arrayStringConcat(a.support_reasons, ',') != e.reasons
    or coalesce(toInt16(a.is_active), -1) != e.active
    or a.registered_learners != e.registered
    or a.readiness_unavailable != e.unknown
    or coalesce(toInt64(a.total_activities), -1) != e.total
    or coalesce(toInt64(a.completed_activities), -1) != e.complete
    or coalesce(toInt64(a.remaining_activities), -1) != e.remaining
    or (
        e.ccx_id = '69'
        and (
            coalesce(a.weakest_practice_subdomain_name, '') != 'Routing'
            or coalesce(a.weakest_practice_subdomain_score, -1) != 0.1
            or coalesce(a.final_pass_rate, -1) != 0.5
        )
    )
    or (e.ccx_id != '69' and a.weakest_practice_subdomain_name is not null)
