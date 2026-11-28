{% macro pss_practice_weaknesses_for_period(
    authorized_classes, period_start, period_end
) %}
    {% if authorized_classes is not defined or not authorized_classes or period_start is not defined or not period_start or period_end is not defined or not period_end %}
        {{
            exceptions.raise_compiler_error(
                "PSS period weaknesses require authorized classes and both UTC bounds"
            )
        }}
    {% endif %}
    {{ pss_practice_weakness_ranking(authorized_classes, period_start, period_end) }}
{% endmacro %}

{% macro pss_practice_weakness_ranking(
    authorized_classes, period_start=none, period_end=none
) %}
    {% set bounded = period_start is not none and period_end is not none %}
    with
        {% if bounded %}
            {{ period_start }} as window_start, {{ period_end }} as window_end,
        {% endif %}
        classes as (
            select institution_id, ccx_id, master_course
            from {{ ref("int_pss_class_scope") }}
            where
                (institution_id, ccx_id)
                in (select institution_id, ccx_id from {{ authorized_classes }})
        ),
        roster as (
            select
                c.institution_id as institution_id,
                c.master_course as master_course,
                r.ccx_id as ccx_id,
                r.actor_id as actor_id
            from {{ ref("int_pss_learner_class_context") }} as r
            inner join classes as c on c.ccx_id = r.ccx_id
            where r.is_registered = 1
        ),
        eligibility_gaps as (
            select
                ccx_id,
                actor_id,
                countIf(
                    is_eligible_practice is null or activity_type is null
                    {% if bounded %}
                        or (is_eligible_practice = 1 and attempted_at is null)
                    {% endif %}
                ) as unresolved
                {% if bounded %}
                    ,
                    countIf(
                        is_eligible_practice = 1
                        and (
                            not is_finished
                            or attempt_finished_at is null
                            or attempt_finished_at >= window_end
                        )
                    ) as missing_period_score
                {% endif %}
            from {{ ref("rpt_pss_activity_attempts") }}
            where
                (institution_id, ccx_id) in (select institution_id, ccx_id from classes)
                and provider = 'measureup'
                and (activity_type = 'Practice Exam' or activity_type is null)
                {% if bounded %}
                    and (
                        attempted_at is null
                        or (attempted_at >= window_start and attempted_at < window_end)
                    )
                {% endif %}
            group by ccx_id, actor_id
        ),
        {% if bounded %}
            learner_certificates as (
                select distinct
                    institution_id, ccx_id, actor_id, snapshot_id, 1 as covered
                from {{ ref("int_pss_readiness_history_coverage") }}
                where
                    activity_type = 'Practice Exam'
                    and (institution_id, ccx_id)
                    in (select institution_id, ccx_id from classes)
                    and (
                        (full_history and checked_at >= window_end)
                        or (starts_at <= window_start and ends_at >= window_end)
                    )
            ),
            certified as (
                select distinct institution_id, ccx_id, subsection_block_id, snapshot_id
                from {{ ref("int_pss_activity_attempt_coverage") }}
                where
                    (institution_id, ccx_id)
                    in (select institution_id, ccx_id from classes)
                    and coverage_start <= window_start
                    and coverage_end >= window_end
            ),
            coverage_gaps as (
                select a.ccx_id as ccx_id, count() as unverified_activities
                from {{ ref("int_pss_activity_catalog") }} as a
                left anti join
                    certified as cert
                    on cert.institution_id = a.institution_id
                    and cert.ccx_id = a.ccx_id
                    and cert.snapshot_id = a.snapshot_id
                    and cert.subsection_block_id = a.subsection_block_id
                where
                    a.in_latest_publish
                    and a.activity_type = 'Practice Exam'
                    and (a.institution_id, a.ccx_id)
                    in (select institution_id, ccx_id from classes)
                group by a.ccx_id
            ),
        {% endif %}
        pooled as (
            select
                ccx_id,
                actor_id,
                domain_id,
                subdomain_id,
                min(subdomain_name) as label,
                sum(correct_items) as correct,
                sum(total_items) as total,
                countIf(
                    coverage_status != 'counted'
                    or (mapping_status != 'verified' and total_items > 0)
                ) as unresolved,
                correct / nullIf(total, 0) as score
            from {{ ref("rpt_pss_practice_subdomain_results") }}
            where
                (institution_id, ccx_id) in (select institution_id, ccx_id from classes)
                {% if bounded %}
                    and attempted_at >= window_start and attempted_at < window_end
                {% endif %}
            group by ccx_id, actor_id, domain_id, subdomain_id
        ),
        ranked as (
            select
                ccx_id,
                actor_id,
                1 as has_practice,
                sum(unresolved) as unresolved_items,
                sum(total) as eligible_items,
                arraySlice(
                    arraySort(
                        x -> (x .6, toUInt64OrNull(x .2), x .2, x .1),
                        groupArrayIf(
                            tuple(
                                assumeNotNull(domain_id),
                                assumeNotNull(subdomain_id),
                                label,
                                correct,
                                total,
                                assumeNotNull(score)
                            ),
                            domain_id is not null
                            and subdomain_id is not null
                            and score < 0.70
                            and unresolved = 0
                        )
                    ),
                    1,
                    3
                ) as observed_weaknesses
            from pooled
            group by ccx_id, actor_id
        )
    select
        r.institution_id as institution_id,
        r.master_course as master_course,
        r.ccx_id as ccx_id,
        r.actor_id as actor_id,
        multiIf(
            {% if bounded %}
                window_start is null
                or window_end is null
                or window_start >= window_end,
                'invalid_window',
                ifNull(ic.inventory_status, '')
                not in ('classified', 'no_graded_subsections')
                or ifNull(pub.publication_status, '') != 'observed'
                or pub.published_time is null
                or pub.published_time > window_start
                or (
                    ifNull(lc.covered, 0) = 0
                    and ifNull(cg.unverified_activities, 0) > 0
                ),
                'interval_unverified',
            {% endif %}
            ifNull(e.unresolved, 0) > 0,
            'eligibility_unavailable',
            {% if bounded %}
                ifNull(e.missing_period_score, 0) > 0,
                'period_score_history_unavailable',
            {% endif %}
            ifNull(p.unresolved_items, 0) > 0,
            'item_or_mapping_unavailable',
            ifNull(p.has_practice, 0) = 0,
            {% if bounded %} 'no_eligible_practice'
            {% else %} 'no_observed_practice'
            {% endif %},
            p.eligible_items = 0,
            'zero_eligible_items',
            'ranked'
        ) as ranking_status,
        if(ranking_status = 'ranked', p.observed_weaknesses, []) as top_weaknesses
    from roster as r
    left join ranked as p on r.ccx_id = p.ccx_id and r.actor_id = p.actor_id
    left join eligibility_gaps as e on r.ccx_id = e.ccx_id and r.actor_id = e.actor_id
    {% if bounded %}
        left join
            {{ ref("int_pss_class_inventory_coverage") }} as ic on r.ccx_id = ic.ccx_id
        left join
            {{ ref("int_pss_master_publication") }} as pub
            on pub.master_course = r.master_course
            and pub.snapshot_id = ic.snapshot_id
        left join
            learner_certificates as lc
            on lc.institution_id = r.institution_id
            and lc.ccx_id = r.ccx_id
            and lc.actor_id = r.actor_id
            and lc.snapshot_id = ic.snapshot_id
        left join coverage_gaps as cg on r.ccx_id = cg.ccx_id
    {% endif %}
{% endmacro %}
