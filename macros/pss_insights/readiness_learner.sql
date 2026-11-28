{# The caller supplies its authorized institution/CCX relation and both UTC SQL bounds. #}
{% macro pss_learner_readiness(authorized_classes, period_start, period_end) %}
    {% if not authorized_classes or not period_start or not period_end %}
        {{
            exceptions.raise_compiler_error(
                "PSS period readiness requires authorized classes and both UTC bounds"
            )
        }}
    {% endif %}
    {{ pss_readiness_query(authorized_classes, period_start, period_end) }}
{% endmacro %}

{% macro pss_readiness_query(authorized_classes, period_start, period_end) %}
    {% set period = period_start is not none %}
    {% set means = var("pss_readiness_mean_activity_masters", []) %}
    {% if means is string or means is mapping or means is not sequence %}
        {{
            exceptions.raise_compiler_error(
                "pss_readiness_mean_activity_masters must be a list of master course keys"
            )
        }}
    {% endif %}
    {% for master in means %}
        {% if master is not string or not master.startswith("course-v1:") %}
            {{
                exceptions.raise_compiler_error(
                    "Invalid master course key for the readiness activity mean"
                )
            }}
        {% endif %}
    {% endfor %}
    {% set approved = tojson(means) | replace("\\", "\\\\") | replace("'", "''") %}
    with
        {% if period %}
            {{ period_start }} as window_start, {{ period_end }} as window_end,
        {% endif %}
        members as (
            select
                assumeNotNull(c.ccx_id) as ccx_id,
                assumeNotNull(c.institution_id) as institution_id,
                c.master_course as master_course,
                c.snapshot_id as snapshot_id,
                c.inventory_status as inventory_status,
                r.actor_id as actor_id
            from {{ ref("int_pss_class_inventory_coverage") }} as c
            inner join
                {{ ref("int_pss_learner_class_context") }} as r on c.ccx_id = r.ccx_id
            where
                r.is_registered = 1
                {% if authorized_classes is not none %}
                    and (c.institution_id, c.ccx_id)
                    in (select institution_id, ccx_id from {{ authorized_classes }})
                {% endif %}
        ),
        by_activity as (
            select
                e.institution_id as institution_id,
                e.ccx_id as ccx_id,
                e.actor_id as actor_id,
                e.subsection_block_id as subsection_block_id,
                arrayJoin(
                    if(
                        e.activity_type is null,
                        ['Quiz', 'Lab', 'Practice Exam'],
                        [ifNull(e.activity_type, '')]
                    )
                ) as activity_type,
                countIf(
                    e.evidence_kind = 'sitting' and e.eligible = 1
                ) as eligible_sittings,
                countIf(
                    e.evidence_kind = 'sitting'
                    and (
                        e.eligible is null or e.activity_type is null
                        {% if period %}
                            or (e.eligible = 1 and e.started_at is null)
                        {% endif %}
                    )
                ) as uncertain_sittings,
                {% if period %}
                    sumIf(
                        ifNull(e.correct_items, 0),
                        e.finished and e.observed_at < window_end
                    ) as correct_items,
                    sumIf(
                        ifNull(e.total_items, 0),
                        e.finished and e.observed_at < window_end
                    ) as total_items,
                    countIf(
                        e.evidence_kind = 'sitting'
                        and e.eligible = 1
                        and (
                            not e.finished
                            or e.observed_at is null
                            or e.observed_at >= window_end
                        )
                    ) as undated_results,
                {% else %}
                    sum(ifNull(e.correct_items, 0)) as correct_items,
                    sum(ifNull(e.total_items, 0)) as total_items,
                {% endif %}
                countIf(
                    e.evidence_kind = 'sitting'
                    and e.eligible = 1
                    and (e.correct_items is null or ifNull(e.total_items, 0) = 0)
                ) as unscored_practice,
                countIf(
                    e.evidence_kind = 'current_score' and e.native_grade_count > 0
                ) as native_observations,
                {% if period %}
                    -- A retired or reclassified activity can have dated sittings
                    -- without any current grade row. Resolve policy by master and type.
                    any(
                        {{
                            pss_activity_score_rule(
                                "e.master_course", "ifNull(e.activity_type, '')"
                            )
                        }}
                    ) as score_rule,
                    maxOrNullIf(
                        e.observed_at,
                        e.evidence_kind = 'sitting'
                        and e.eligible = 1
                        and e.finished
                        and e.observed_at < window_end
                    ) as latest_finish,
                    groupArrayIf(
                        tuple(e.finished, e.observed_at, e.score_pct),
                        e.evidence_kind = 'sitting' and e.eligible = 1
                    ) as scored_sittings,
                    arrayMap(
                        x -> if(x .1 and x .2 < window_end, x .3, null),
                        arrayFilter(
                            x -> score_rule != 'measureup_latest_finished'
                            or (x .1 and x .2 = latest_finish),
                            scored_sittings
                        )
                    ) as dated_scores,
                    if(
                        score_rule = 'lms_subsection'
                        or native_observations > 0
                        and score_rule != 'measureup_latest_finished'
                        or empty(dated_scores)
                        or arrayExists(x -> isNull(x), dated_scores)
                        or arrayUniq(dated_scores) != 1,
                        null,
                        dated_scores[1]
                    ) as activity_score_pct
                {% else %}
                    if(
                        hasAll(
                            groupUniqArrayIf(
                                e.source_key,
                                e.evidence_kind = 'sitting' and e.eligible = 1
                            ),
                            anyIf(
                                e.selected_provider_attempt_keys,
                                e.evidence_kind = 'current_score'
                            )
                        ),
                        maxOrNullIf(e.score_pct, e.evidence_kind = 'current_score'),
                        null
                    ) as activity_score_pct
                {% endif %}
            from {{ ref("int_pss_readiness_evidence") }} as e
            {% if period %}
                where
                    (e.institution_id, e.ccx_id)
                    in (select institution_id, ccx_id from {{ authorized_classes }})
                    and (
                        e.evidence_kind = 'current_score'
                        or e.started_at is null
                        or (e.started_at >= window_start and e.started_at < window_end)
                    )
            {% endif %}
            group by
                e.institution_id,
                e.ccx_id,
                e.actor_id,
                e.subsection_block_id,
                activity_type
        ),
        by_type as (
            select
                institution_id,
                ccx_id,
                actor_id,
                activity_type,
                sum(eligible_sittings) as sitting_count,
                sum(uncertain_sittings) as uncertain_count,
                countIf(eligible_sittings > 0) as activity_count,
                countIf(
                    native_observations > 0 and eligible_sittings = 0
                ) as unlinked_native,
                sumIf(
                    toDecimal256OrZero(toString(ifNull(activity_score_pct, 0)), 18),
                    eligible_sittings > 0
                ) as sum_score_pct,
                uniqExactIf(
                    activity_score_pct,
                    eligible_sittings > 0 and activity_score_pct is not null
                ) as distinct_scores,
                countIf(
                    eligible_sittings > 0 and activity_score_pct is null
                ) as unscored_activities,
                sum(correct_items) as correct_count,
                sum(total_items) as total_count,
                {% if period %} sum(undated_results) as undated_result_count,{% endif %}
                sum(unscored_practice) as unscored_practice_count
            from by_activity
            group by institution_id, ccx_id, actor_id, activity_type
        ),
        verified_memberships as (
            select distinct
                institution_id,
                ccx_id,
                actor_id,
                snapshot_id,
                activity_type,
                1 as covered
            from {{ ref("int_pss_readiness_history_coverage") }}
            where
                {% if period %}
                    (institution_id, ccx_id)
                    in (select institution_id, ccx_id from {{ authorized_classes }})
                    and (
                        (full_history and checked_at >= window_end)
                        or (starts_at <= window_start and ends_at >= window_end)
                    )
                {% else %} full_history
                {% endif %}
        ),
        {% if period %}
            current_activities as (
                select
                    a.institution_id as institution_id,
                    a.ccx_id as ccx_id,
                    a.activity_type as activity_type,
                    a.subsection_block_id as subsection_block_id,
                    max(
                        ifNull(
                            w.coverage_start <= window_start
                            and w.coverage_end >= window_end,
                            0
                        )
                    ) as covered
                from {{ ref("int_pss_activity_catalog") }} as a
                inner join
                    {{ ref("int_pss_master_publication") }} as pub
                    on a.master_course = pub.master_course
                    and a.snapshot_id = pub.snapshot_id
                left join
                    {{ ref("int_pss_activity_attempt_coverage") }} as w
                    on a.ccx_id = w.ccx_id
                    and a.snapshot_id = w.snapshot_id
                    and a.subsection_block_id = w.subsection_block_id
                where
                    a.in_latest_publish
                    and pub.published_time <= window_start
                    and (a.institution_id, a.ccx_id)
                    in (select institution_id, ccx_id from {{ authorized_classes }})
                group by
                    a.institution_id, a.ccx_id, a.activity_type, a.subsection_block_id
            ),
            verified_types as (
                select institution_id, ccx_id, activity_type, min(covered) as covered
                from current_activities
                group by institution_id, ccx_id, activity_type
            ),
        {% endif %}
        population as (
            select *, arrayJoin(['Quiz', 'Lab', 'Practice Exam']) as activity_type
            from members
        ),
        contributions as (
            select
                p.institution_id as institution_id,
                p.ccx_id as ccx_id,
                p.master_course as master_course,
                p.actor_id as actor_id,
                p.activity_type as activity_type,
                ifNull(b.sitting_count, 0) as sitting_count,
                ifNull(b.activity_count, 0) as activity_count,
                w.weight as course_weight,
                notEmpty(ifNull(w.activity_type, '')) as has_weight,
                (
                    ifNull(v.covered, 0) = 1
                    {% if period %} or ifNull(t.covered, 0) = 1{% endif %}
                ) as telemetry_verified,
                ifNull(b.correct_count, 0) as correct_count,
                ifNull(b.total_count, 0) as total_count,
                if(
                    p.activity_type = 'Practice Exam',
                    toDecimal256(correct_count, 18),
                    ifNull(b.sum_score_pct, 0)
                ) as numerator,
                if(
                    p.activity_type = 'Practice Exam', total_count, activity_count * 100
                ) as denominator,
                multiIf(
                    {% if period %}
                        window_start is null
                        or window_end is null
                        or window_start >= window_end,
                        'invalid_period',
                    {% endif %}
                    p.inventory_status not in ('classified', 'no_graded_subsections'),
                    'inventory_unavailable',
                    has_weight and course_weight = 0,
                    if(
                        sitting_count = 0 and not telemetry_verified,
                        'zero_weight_unverified',
                        'zero_weight'
                    ),
                    ifNull(b.uncertain_count, 0) > 0,
                    'eligibility_unavailable',
                    p.activity_type != 'Practice Exam'
                    and ifNull(b.unlinked_native, 0) > 0,
                    {% if period %} 'period_score_history_unavailable'
                    {% else %} 'whole_attempt_unverified'
                    {% endif %},
                    sitting_count > 0 and not has_weight,
                    'weight_unavailable',
                    {% if period %}
                        p.activity_type = 'Practice Exam'
                        and ifNull(b.undated_result_count, 0) > 0,
                        'period_score_history_unavailable',
                    {% endif %}
                    p.activity_type = 'Practice Exam'
                    and sitting_count > 0
                    and (ifNull(b.unscored_practice_count, 0) > 0 or denominator = 0),
                    'practice_item_coverage_unavailable',
                    p.activity_type != 'Practice Exam'
                    and ifNull(b.unscored_activities, 0) > 0,
                    {% if period %} 'period_score_history_unavailable'
                    {% else %} 'attempt_score_unavailable'
                    {% endif %},
                    p.activity_type != 'Practice Exam'
                    and ifNull(b.distinct_scores, 0) > 1
                    and not has(
                        JSONExtract('{{ approved }}', 'Array(String)'), p.master_course
                    ),
                    'activity_aggregation_unapproved',
                    not telemetry_verified,
                    'telemetry_unverified',
                    sitting_count = 0,
                    'no_eligible_attempts',
                    'contributed'
                ) as contribution_status
            from population as p
            left join
                by_type as b using (institution_id, ccx_id, actor_id, activity_type)
            left join
                verified_memberships as v
                on p.institution_id = v.institution_id
                and p.ccx_id = v.ccx_id
                and p.actor_id = v.actor_id
                and p.snapshot_id = v.snapshot_id
                and p.activity_type = v.activity_type
            {% if period %}
                left join
                    verified_types as t
                    on p.institution_id = t.institution_id
                    and p.ccx_id = t.ccx_id
                    and p.activity_type = t.activity_type
            {% endif %}
            left join
                {{ ref("int_pss_grading_policy") }} as w
                on p.master_course = w.master_course
                and p.activity_type = w.activity_type
        )
        {{ pss_readiness_fold("contributions") }}
{% endmacro %}
