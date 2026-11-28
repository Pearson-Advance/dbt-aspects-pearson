{{ config(tags=["pss_reporting", "pss_reporting_fixture"]) }}

-- A: 1/1. B: 1/4. Shared learner Amy passes in A and fails in B.
-- Unique people: Amy and Ben pass, Cal and Dee fail. 2/4, not (1 + .25)/2.
with
    class_fixture as (
        select
            *,
            if(
                master_course = 'partial',
                null,
                toUInt64(multiIf(ccx_id = 'A', 1, ccx_id = 'B', 4, ccx_id = 'D', 1, 0))
            ) as registered_learners
        from
        values
            (
                'institution_id String, ccx_id String, master_course String, class_risk Nullable(String), class_start Nullable(Date), class_end Nullable(Date)',
                ('north', 'A', 'course', 'Low', '2026-11-19', '2026-12-01'),
                ('south', 'B', 'course', 'High', '2026-10-01', '2026-11-19'),
                ('south', 'private', 'course', 'High', '2026-10-01', '2026-12-01'),
                ('south', 'ended', 'course', 'Medium', '2026-10-01', '2026-11-18'),
                ('south', 'undated', 'course', 'High', null, '2026-12-01'),
                ('north', 'C', 'empty', null, '2026-11-19', '2026-11-19'),
                ('north', 'D', 'unknown', null, '2026-11-19', '2026-11-19'),
                ('south', 'P', 'partial', null, '2026-11-19', '2026-11-19')
            )
    ),
    membership_fixture as (
        select *
        from
        values
            (
                'institution_id String, ccx_id String, master_course String, actor_id String, readiness_status Nullable(String), final_attempted Nullable(UInt8), final_passed Nullable(UInt8)',
                ('north', 'A', 'course', 'Amy', 'On Target', 1, 1),
                ('south', 'B', 'course', 'Amy', 'At Risk', 1, 0),
                ('south', 'B', 'course', 'Ben', 'On Target', 1, 1),
                ('south', 'B', 'course', 'Cal', 'Borderline', 1, 0),
                ('south', 'B', 'course', 'Dee', null, 1, 0),
                ('south', 'private', 'course', 'Eva', 'On Target', 1, 1),
                ('south', 'ended', 'course', 'Fay', 'On Target', 1, 1),
                ('north', 'D', 'unknown', 'Gus', null, null, null),
                ('south', 'P', 'partial', 'Known', 'On Target', 1, 1)
            )
    ),
    access_fixture as (
        select *
        from
        values
            (
                'institution_id String, ccx_id String, instructor String',
                ('north', 'A', 'Jones'),
                ('north', 'A', 'Smith'),
                ('south', 'B', 'Jones'),
                ('north', 'private', 'Jones'),
                ('south', 'ended', 'Jones'),
                ('south', 'undated', 'Smith'),
                ('north', 'C', 'Jones'),
                ('north', 'D', 'Smith'),
                ('south', 'P', 'Jones')
            )
    ),
    summary as (
        {{
            pss_master_course_summary(
                "access_fixture",
                "toDate('2026-11-19')",
                "class_fixture",
                "membership_fixture",
            )
        }}
    )
select 'scoped distinct learners, dates, pass rate, or unavailable evidence' as error
where
    (
        select count()
        from summary
        where
            master_course = 'course'
            and active_classes = 2
            and classes_with_unavailable_dates = 1
            and classes_requiring_attention = 1
            and registered_learners = 4
            and readiness_available = 2
            and readiness_unavailable = 2
            and learners_with_missing_readiness = 1
            and learners_with_conflicting_readiness = 1
            and pct_on_target = 0.5
            and final_attempted_learners = 4
            and final_passed_learners = 2
            and final_pass_rate = 0.5
    )
    != 1
    or (
        select count()
        from summary
        where
            master_course = 'empty'
            and active_classes = 1
            and registered_learners = 0
            and final_pass_rate is null
            and pct_on_target is null
    )
    != 1
    or (
        select count()
        from summary
        where
            master_course = 'unknown'
            and registered_learners = 1
            and final_unresolved_learners = 1
            and final_pass_rate is null
    )
    != 1
    or (
        select count()
        from summary
        where
            master_course = 'partial'
            and registered_learners is null
            and observed_registered_learners = 1
            and classes_with_unverified_rosters = 1
            and pct_on_target is null
            and final_pass_rate is null
            and final_passed_learners = 1
    )
    != 1
