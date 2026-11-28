{{ config(tags=["pss_readiness"]) }}

-- All three eligible types exist on the roster spine, even for an idle member.
with
    examples as (
        select *
        from
            -- fmt: off
            values(
                'actor_id String, quiz_pct Decimal(18,8), practice_correct UInt64, practice_total UInt64, quiz_weight Float64, practice_weight Float64, quiz_status String, practice_status String, expected_score Nullable(Float64), expected_band Nullable(String), expected_reason Nullable(String)',
                ('weighted', 60, 9, 12, 10, 20, 'contributed', 'contributed', 0.70, 'On Target', null),
                ('recurring', 60, 5, 6, 4, 3, 'contributed', 'contributed', 0.70, 'On Target', null),
                ('zero', 0, 0, 0, 10, 20, 'contributed', 'no_eligible_attempts', 0.0, 'At Risk', null),
                ('below_60', '59.99', 0, 0, 10, 20, 'contributed', 'no_eligible_attempts', 0.5999, 'At Risk', null),
                ('at_60', 60, 0, 0, 10, 20, 'contributed', 'no_eligible_attempts', 0.60, 'Borderline', null),
                ('above_60', '60.01', 0, 0, 10, 20, 'contributed', 'no_eligible_attempts', 0.6001, 'Borderline', null),
                ('below_70', '69.99', 0, 0, 10, 20, 'contributed', 'no_eligible_attempts', 0.6999, 'Borderline', null),
                ('at_70', 70, 0, 0, 10, 20, 'contributed', 'no_eligible_attempts', 0.70, 'On Target', null),
                ('above_70', '70.01', 0, 0, 10, 20, 'contributed', 'no_eligible_attempts', 0.7001, 'On Target', null),
                ('only_zero_weight', 0, 9, 12, 10, 0, 'no_eligible_attempts', 'zero_weight', null, null, 'zero_eligible_weight'),
                ('final_only', 0, 0, 0, 10, 20, 'no_eligible_attempts', 'no_eligible_attempts', null, null, 'no_eligible_attempts'),
                ('weight_missing', 0, 9, 12, 0, 20, 'weight_unavailable', 'contributed', null, null, 'weight_unavailable'),
                ('unscored', 60, 0, 0, 10, 20, 'contributed', 'practice_item_coverage_unavailable', null, null, 'practice_item_coverage_unavailable'),
                ('unknown', 60, 0, 0, 10, 20, 'contributed', 'telemetry_unverified', null, null, 'telemetry_unverified')
            )
            -- fmt: on
    ),
    contributions as (
        select
            'I' as institution_id,
            'C' as ccx_id,
            'M' as master_course,
            actor_id,
            arrayJoin(['Quiz', 'Lab', 'Practice Exam']) as activity_type,
            multiIf(
                activity_type = 'Quiz',
                quiz_status,
                activity_type = 'Practice Exam',
                practice_status,
                'no_eligible_attempts'
            ) as contribution_status,
            toUInt64(
                contribution_status
                not in ('no_eligible_attempts', 'telemetry_unverified')
            ) as sitting_count,
            sitting_count as activity_count,
            multiIf(
                activity_type = 'Quiz',
                quiz_weight,
                activity_type = 'Practice Exam',
                practice_weight,
                0
            ) as course_weight,
            contribution_status != 'weight_unavailable' as has_weight,
            if(activity_type = 'Practice Exam', practice_correct, 0) as correct_count,
            if(activity_type = 'Practice Exam', practice_total, 0) as total_count,
            if(
                activity_type = 'Practice Exam',
                toDecimal256(practice_correct, 18),
                toDecimal256(quiz_pct, 18)
            ) as numerator,
            if(activity_type = 'Practice Exam', practice_total, 100) as denominator
        from examples
    ),
    results as (
        with inputs as (select * from contributions) {{ pss_readiness_fold("inputs") }}
    )
select e.actor_id
from examples as e
left join results as r on e.actor_id = r.actor_id
where
    ifNull(r.actor_id, '') != e.actor_id
    or ifNull(r.readiness_status, '') != ifNull(e.expected_band, '')
    or ifNull(r.readiness_unavailable_reason, '') != ifNull(e.expected_reason, '')
    or isNull(r.readiness_score) != isNull(e.expected_score)
    or abs(r.readiness_score - e.expected_score) > 1e-12
