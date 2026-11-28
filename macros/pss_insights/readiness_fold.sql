{% macro pss_readiness_fold(contributions) %}
    {% set types = [("quiz", "Quiz"), ("lab", "Lab"), ("practice", "Practice Exam")] %},
    -- Carry the three independent denominators into the comparison. This preserves
    -- an exact 0.70 when the pooled practice ratio has a repeating decimal.
    learner as (
        select
            institution_id,
            ccx_id,
            master_course,
            actor_id,
            {% for prefix, label in types %}
                anyIf(
                    contribution_status, activity_type = '{{ label }}'
                ) as {{ prefix }}_status,
                maxIf(
                    sitting_count, activity_type = '{{ label }}'
                ) as {{ prefix }}_sittings,
                maxIf(
                    activity_count, activity_type = '{{ label }}'
                ) as {{ prefix }}_activities,
                maxOrNullIf(
                    course_weight, activity_type = '{{ label }}' and has_weight
                ) as {{ prefix }}_weight,
                maxIf(
                    if(
                        contribution_status = 'contributed',
                        numerator,
                        toDecimal256(0, 18)
                    ),
                    activity_type = '{{ label }}'
                ) as {{ prefix }}_n,
                maxIf(
                    if(contribution_status = 'contributed', denominator, 1),
                    activity_type = '{{ label }}'
                ) as {{ prefix }}_d,
                maxIf(
                    if(
                        contribution_status = 'contributed',
                        toDecimal256OrZero(toString(ifNull(course_weight, 0)), 18),
                        toDecimal256(0, 18)
                    ),
                    activity_type = '{{ label }}'
                ) as {{ prefix }}_w,
            {% endfor %}
            maxIf(
                correct_count, activity_type = 'Practice Exam'
            ) as observed_practice_correct_items,
            maxIf(
                total_count, activity_type = 'Practice Exam'
            ) as observed_practice_total_items
        from {{ contributions }}
        group by institution_id, ccx_id, master_course, actor_id
    ),
    calculated as (
        select
            *,
            quiz_w + lab_w + practice_w as weight_sum,
            toDecimal256(quiz_d, 0) * lab_d * practice_d as common_denominator,
            quiz_w * quiz_n * lab_d * practice_d
            + lab_w * lab_n * quiz_d * practice_d
            + practice_w * practice_n * quiz_d * lab_d as weighted_n,
            weight_sum * common_denominator as weighted_d,
            arrayFirst(
                s -> has([quiz_status, lab_status, practice_status], s),
                [
                    'invalid_period',
                    'inventory_unavailable',
                    'eligibility_unavailable',
                    'whole_attempt_unverified',
                    'weight_unavailable',
                    'period_score_history_unavailable',
                    'attempt_score_unavailable',
                    'practice_item_coverage_unavailable',
                    'activity_aggregation_unapproved',
                    'telemetry_unverified'
                ]
            ) as blocker,
            nullIf(
                multiIf(
                    notEmpty(blocker),
                    blocker,
                    weight_sum = 0
                    and quiz_sittings + lab_sittings + practice_sittings > 0,
                    'zero_eligible_weight',
                    weight_sum = 0
                    and has(
                        [quiz_status, lab_status, practice_status],
                        'zero_weight_unverified'
                    ),
                    'telemetry_unverified',
                    weight_sum = 0,
                    'no_eligible_attempts',
                    ''
                ),
                ''
            ) as readiness_unavailable_reason
        from learner
    )
    select
        institution_id,
        ccx_id,
        master_course,
        actor_id,
        if(
            readiness_unavailable_reason is null,
            toFloat64(divideDecimal(weighted_n, nullIf(weighted_d, 0), 18)),
            null
        ) as readiness_score,
        multiIf(
            readiness_unavailable_reason is not null,
            null,
            weighted_n * 10 >= weighted_d * 7,
            'On Target',
            weighted_n * 10 >= weighted_d * 6,
            'Borderline',
            'At Risk'
        ) as readiness_status,
        readiness_unavailable_reason,
        toFloat64(weight_sum) as contributing_weight,
        {% for prefix, label in types %}
            {{ prefix }}_status,
            {{ prefix }}_sittings,
            {{ prefix }}_activities,
            {{ prefix }}_weight,
            if(
                {{ prefix }}_status = 'contributed',
                toFloat64({{ prefix }}_n) / nullIf({{ prefix }}_d, 0),
                null
            ) as {{ prefix }}_score,
        {% endfor %}
        observed_practice_correct_items,
        observed_practice_total_items
    from calculated
{% endmacro %}
