{{ config(tags=["pss_reporting", "pss_reporting_fixture"]) }}

-- Keys 68 through 81 have 100 learners with available readiness and one
-- without readiness. Classes 2 and 3 exercise missing readiness and empty
-- rosters. Class 1 has a complete, genuinely empty inventory.
with
    classes as (
        select
            toString(
                arrayJoin([1, 2, 3, 4, 5, 6, 67, 68, 69, 70, 71, 79, 80, 81])
            ) as ccx_id,
            'institution' as institution_id,
            ccx_id as institution_ccx_id,
            'master' as master_course,
            ccx_id as class_name,
            'in_scope' as class_scope_status,
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
            toDecimal128(
                multiIf(
                    c.ccx_id = '68', '0.70', c.ccx_id = '67', '0.69999999999999', '0.69'
                ),
                18
            ) as quiz_score_sum,
            1 as quiz_score_count,
            toDecimal128(if(c.ccx_id = '69', '0.65', '0'), 18) as lab_score_sum,
            toUInt8(c.ccx_id = '69') as lab_score_count,
            toDecimal128(
                multiIf(c.ccx_id in ('67', '68', '69'), '0.7', '0.5'), 18
            ) as practice_score_sum,
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
        select *, 'institution' as institution_id, 'master' as master_course
        from
        values
            (
                'ccx_id String, actor_id String, domain_id Nullable(String), subdomain_id Nullable(String), subdomain_name Nullable(String), correct_items Nullable(UInt64), total_items Nullable(UInt64), mapping_status String, coverage_status String',
                ('69', '0', 'D', 'A', 'Routing', 1, 1, 'verified', 'counted'),
                ('69', '1', 'D', 'A', 'Routing', 0, 9, 'verified', 'counted'),
                ('69', '0', 'D', 'B', 'Security', 1, 4, 'verified', 'counted'),
                ('69', 'visitor', 'D', 'A', 'Routing', 99, 99, 'verified', 'counted'),
                ('5', '0', 'D', 'A', 'Routing', 0, 1, 'verified', 'counted'),
                ('6', '0', 'D', 'A', 'Routing', 0, 1, 'verified', 'counted'),
                ('70', '0', 'D', 'A', 'Routing', 0, 1, 'verified', 'counted'),
                ('70', '1', null, null, null, 1, 1, 'objective_unavailable', 'counted'),
                ('71', '0', 'D', 'A', 'Routing', 0, 1, 'verified', 'counted'),
                ('80', '0', 'D', 'A', 'Routing', 0, 1, 'verified', 'counted'),
                ('81', '0', 'D', 'A', 'Routing', 0, 1, 'verified', 'counted'),
                (
                    '81',
                    '1',
                    'D',
                    'B',
                    'Security',
                    null,
                    null,
                    'verified',
                    'item_policy_required'
                )
            )
        union all
        -- Objective 4 is fourth for each learner. Their three lowest objectives
        -- differ, so objective 4 is the weakest after pooling the full detail.
        select
            '68' as ccx_id,
            toString(learner) as actor_id,
            'D' as domain_id,
            toString(objective) as subdomain_id,
            concat('Objective ', subdomain_id) as subdomain_name,
            multiIf(
                objective = 4, 5, (objective < 4) = (learner = 0), 0, 100
            ) as correct_items,
            multiIf(
                objective = 4, 10, (objective < 4) = (learner = 0), 1, 100
            ) as total_items,
            'verified' as mapping_status,
            'counted' as coverage_status,
            'institution' as institution_id,
            'master' as master_course
        from (select number as learner from numbers(2)) as actors
        cross join (select number as objective from numbers(1, 7)) as objectives
    ),
    practice_ranks as (
        select
            ccx_id,
            institution_id,
            master_course,
            actor_id,
            if(
                ccx_id = '80' and actor_id = '1', 'eligibility_unavailable', 'ranked'
            ) as ranking_status
        from learners
    ),
    practice_history as (
        select
            ccx_id,
            institution_id,
            actor_id,
            'v1' as snapshot_id,
            'Practice Exam' as activity_type,
            1 as full_history
        from learners
        where ccx_id != '71' or actor_id != '0'
    ),
    unlinked as (
        select *
        from
        values
            (
                'institution_id String, ccx_id String, actor_id String, activity_type Nullable(String)',
                ('institution', '5', '0', null),
                ('institution', '6', '0', 'Practice Exam'),
                ('institution', '68', '0', 'Quiz'),
                ('other', '68', '0', 'Practice Exam'),
                ('institution', '69', 'visitor', null)
            )
    ),
    catalogs as (
        select
            c.ccx_id as ccx_id,
            c.institution_id as institution_id,
            c.master_course as master_course,
            'v1' as snapshot_id,
            if(c.ccx_id = '4', 'identity_incomplete', 'resolved') as roster_status,
            toUInt64(
                multiIf(
                    c.ccx_id = '4',
                    102,
                    c.ccx_id = '3',
                    0,
                    c.ccx_id in ('1', '2'),
                    1,
                    101
                )
            ) as registered_source_learners,
            toUInt64(c.ccx_id = '4') as unresolved_registered_learners,
            if(
                c.ccx_id = '4', null, registered_source_learners
            ) as registered_actor_denominator,
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
                "practice_ranks",
                "practice_history",
                "unlinked",
            )
        }}
    ),
    expected as (
        select *
        from
        values
            (
                'ccx_id String, risk String, attention Int8, support Int8, reasons String, active Int8, registered Int64, unknown UInt64, total Int8, complete Int8, remaining Int8',
                ('1', 'Low', 0, 0, '', 1, 1, 0, 0, 0, 0),
                ('2', 'NA', -1, -1, '', 1, 1, 1, 1, 0, 1),
                ('3', 'NA', -1, -1, '', -1, 0, 0, 1, -1, -1),
                ('4', 'NA', -1, -1, '', 1, -1, 1, 1, -1, -1),
                ('5', 'High', 1, 1, 'Quiz,Practice Exam', 1, 101, 1, 1, 0, 1),
                ('6', 'High', 1, 1, 'Quiz,Practice Exam', 1, 101, 1, 1, 0, 1),
                ('67', 'High', 1, 1, 'Quiz', 1, 101, 1, 1, 0, 1),
                ('68', 'High', 1, 0, '', 1, 101, 1, 1, 0, 1),
                ('69', 'High', 1, 1, 'Quiz,Lab', 1, 101, 1, 1, 1, 0),
                ('70', 'Medium', 1, 0, '', 1, 101, 1, 1, 0, 1),
                ('71', 'Medium', 1, 0, '', 1, 101, 1, 1, -1, -1),
                ('79', 'Medium', 1, 0, '', 1, 101, 1, -1, -1, -1),
                ('80', 'Low', 0, 0, '', 1, 101, 1, 1, 0, 1),
                ('81', 'Low', 0, 0, '', -1, 101, 1, 1, 0, 1)
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
    or ifNull(toInt64(a.registered_learners), -1) != e.registered
    or (
        e.ccx_id = '4'
        and (
            a.observed_registered_learners != 101
            or a.unresolved_registered_learners != 1
            or a.final_pass_rate is not null
        )
    )
    or a.readiness_unavailable != e.unknown
    or coalesce(toInt64(a.total_activities), -1) != e.total
    or coalesce(toInt64(a.completed_activities), -1) != e.complete
    or coalesce(toInt64(a.remaining_activities), -1) != e.remaining
    or (
        e.ccx_id = '69'
        and (
            coalesce(a.weakest_practice_subdomain_name, '') != 'Routing'
            or coalesce(a.weakest_practice_subdomain_score, -1) != 0.1
            or coalesce(a.weakest_practice_correct_items, 0) != 1
            or coalesce(a.weakest_practice_total_items, 0) != 10
            or coalesce(a.final_pass_rate, -1) != 0.5
        )
    )
    or (
        e.ccx_id = '68'
        and (
            ifNull(a.weakest_practice_subdomain_id, '') != '4'
            or ifNull(a.weakest_practice_correct_items, 0) != 10
            or ifNull(a.weakest_practice_total_items, 0) != 20
        )
    )
    or (e.ccx_id not in ('68', '69') and a.weakest_practice_subdomain_name is not null)
    or a.weakest_practice_status != multiIf(
        e.ccx_id in ('68', '69'),
        'ranked',
        e.ccx_id = '71',
        'practice_history_unverified',
        e.ccx_id in ('5', '6', '70', '80', '81'),
        'eligibility_or_item_unavailable',
        e.ccx_id = '79',
        'inventory_unavailable',
        e.ccx_id = '4',
        'roster_unverified',
        e.ccx_id = '3',
        'empty_roster',
        'no_eligible_items'
    )
