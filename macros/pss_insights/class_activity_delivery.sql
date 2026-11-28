{# UTC starts are inclusive and ends are exclusive. Persist the all-class snapshot
   once; a period consumer supplies its authorized institution and CCX relation. #}
{% macro pss_class_activity_delivery(
    period_start=none, period_end=none, authorized_classes=none
) %}
    {% if (
        period_start is not none or period_end is not none
    ) and not authorized_classes %}
        {{
            exceptions.raise_compiler_error(
                "PSS period delivery requires an authorized institution/CCX relation"
            )
        }}
    {% endif %}
    {{
        pss_activity_delivery_query(
            ref("int_pss_class_catalog_coverage"),
            ref("int_pss_activity_catalog"),
            ref("int_pss_reporting_roster"),
            ref("rpt_pss_activity_attempts"),
            ref("rpt_pss_activity_scores_current"),
            ref("int_pss_reporting_coverage"),
            period_start,
            period_end,
            authorized_classes,
            ref("int_pss_reporting_unlinked_attempts"),
        )
    }}
{% endmacro %}

{% macro pss_activity_delivery_query(
    classes,
    catalog,
    roster,
    attempts,
    scores,
    telemetry,
    period_start=none,
    period_end=none,
    authorized_classes=none,
    unlinked_attempts=none
) %}
    {% set bounded = period_start is not none or period_end is not none %}
    with
        {% if bounded %}
            {% if period_start is not none %}
                {{ period_start }} as window_start,
            {% endif %}
            {% if period_end is not none %} {{ period_end }} as window_end,{% endif %}
        {% endif %}
        scoped_classes as (
            select *
            from {{ classes }}
            {% if authorized_classes is not none %}
                where
                    (institution_id, ccx_id)
                    in (select institution_id, ccx_id from {{ authorized_classes }})
            {% endif %}
        ),
        eligible_members as (
            select ccx_id, actor_id
            from {{ roster }}
            {% if authorized_classes is not none %}
                where ccx_id in (select ccx_id from scoped_classes)
            {% endif %}
        ),
        -- An unresolved sitting matters only while it could change the actor count.
        -- The lower bound remains available when an interval or eligibility is unknown.
        participation as (
            select
                a.institution_id as institution_id,
                a.ccx_id as ccx_id,
                a.subsection_block_id as subsection_block_id,
                groupUniqArrayIf(
                    a.actor_id,
                    a.is_participation = 1
                    {% if bounded %} and a.attempted_at is not null{% endif %}
                ) as confirmed_actors,
                length(confirmed_actors) as observed_attempted_learners,
                uniqExact(a.actor_id)
                > observed_attempted_learners as has_additional_candidate,
                countIf(
                    a.is_participation is null
                    {% if bounded %} or a.attempted_at is null {% endif %}
                ) as unresolved_attempts,
                avgOrNullIf(
                    a.score_pct,
                    a.is_participation = 1 and a.score_pct between 0 and 100
                    {% if bounded %} and a.attempted_at is not null{% endif %}
                ) as avg_attempt_score_pct
            from {{ attempts }} as a
            where
                (a.ccx_id, a.actor_id)
                in (select ccx_id, actor_id from eligible_members)
                and (a.is_participation = 1 or a.is_participation is null)
                {% if authorized_classes is not none %}
                    and (a.institution_id, a.ccx_id)
                    in (select institution_id, ccx_id from {{ authorized_classes }})
                {% endif %}
                {% if period_start is not none %}
                    and (a.attempted_at is null or a.attempted_at >= window_start)
                {% endif %}
                {% if period_end is not none %}
                    and (a.attempted_at is null or a.attempted_at < window_end)
                {% endif %}
            group by a.institution_id, a.ccx_id, a.subsection_block_id
        ),
        {% if unlinked_attempts is not none %}
            unlinked as (
                select
                    institution_id,
                    ccx_id,
                    groupUniqArray(
                        tuple(ifNull(subsection_block_id, ''), actor_id)
                    ) as candidates
                from {{ unlinked_attempts }}
                where
                    true
                    {% if authorized_classes is not none %}
                        and (institution_id, ccx_id)
                        in (select institution_id, ccx_id from {{ authorized_classes }})
                    {% endif %}
                    {% if period_start is not none %}
                        and (attempted_at is null or attempted_at >= window_start)
                    {% endif %}
                    {% if period_end is not none %}
                        and (attempted_at is null or attempted_at < window_end)
                    {% endif %}
                group by institution_id, ccx_id
            ),
        {% endif %}
        difficulty as (
            select
                s.institution_id as institution_id,
                s.ccx_id as ccx_id,
                s.subsection_block_id as subsection_block_id,
                avgOrNullIf(s.score_pct, s.score_pct between 0 and 100) as avg_score_pct
            from {{ scores }} as s
            where
                (s.ccx_id, s.actor_id)
                in (select ccx_id, actor_id from eligible_members)
                {% if authorized_classes is not none %}
                    and (s.institution_id, s.ccx_id)
                    in (select institution_id, ccx_id from {{ authorized_classes }})
                {% endif %}
            group by s.institution_id, s.ccx_id, s.subsection_block_id
        ),
        certified as (
            select distinct
                institution_id,
                ccx_id,
                subsection_block_id,
                snapshot_id,
                1 as is_certified
            from {{ telemetry }}
            where
                {% if period_start is not none and period_end is not none %}
                    (
                        (coverage_start <= window_start and coverage_end >= window_end)
                        or (full_history and verified_through >= window_end)
                    )
                {% elif period_end is not none %}
                    full_history and verified_through >= window_end
                {% else %} full_history
                {% endif %}
                {% if authorized_classes is not none %}
                    and (institution_id, ccx_id)
                    in (select institution_id, ccx_id from {{ authorized_classes }})
                {% endif %}
        ),
        evidence as (
            select
                c.ccx_id as ccx_id,
                c.institution_id as institution_id,
                c.master_course as master_course,
                a.snapshot_id as snapshot_id,
                a.subsection_block_id as subsection_block_id,
                a.subsection_name as subsection_name,
                a.activity_type as activity_type,
                a.section_course_order as section_course_order,
                a.subsection_course_order as subsection_course_order,
                a.activity_sequence as activity_sequence,
                c.catalog_status as catalog_status,
                c.roster_status as roster_status,
                c.registered_actor_denominator as registered_learners,
                c.resolved_registered_learners as observed_registered_learners,
                ifNull(p.observed_attempted_learners, 0) as observed_attempted_learners,
                ifNull(p.unresolved_attempts, 0) as unresolved_attempts,
                {% if unlinked_attempts is not none %}
                    arrayFilter(
                        x -> empty(x .1) or x .1 = a.subsection_block_id, u.candidates
                    )
                {% else %} cast([], 'Array(Tuple(String,String))')
                {% endif %} as candidate_sittings,
                notEmpty(candidate_sittings) as has_unlinked_attempt,
                ifNull(p.has_additional_candidate, false)
                or arrayExists(
                    x -> not has(p.confirmed_actors, x .2), candidate_sittings
                ) as has_unresolved_participant,
                ifNull(t.is_certified, 0) as has_coverage_claim,
                multiIf(
                    {% if bounded %}
                        {% if period_start is not none %}
                            window_start is null or
                        {% endif %}
                        {% if period_end is not none %} window_end is null or{% endif %}
                        {% if period_start is not none and period_end is not none %}
                            window_start >= window_end or
                        {% endif %}
                        false,
                        'invalid_window',
                    {% endif %}
                    registered_learners is null,
                    'roster_unverified',
                    unresolved_attempts > 0 or has_unlinked_attempt,
                    'unresolved_attempts',
                    not has_coverage_claim,
                    'telemetry_unverified',
                    'verified'
                ) as participation_status,
                participation_status = 'verified' as telemetry_complete,
                d.avg_score_pct as avg_score_pct,
                p.avg_attempt_score_pct as avg_attempt_score_pct
            from {{ catalog }} as a
            inner join
                scoped_classes as c
                on c.institution_id = a.institution_id
                and c.ccx_id = a.ccx_id
                and c.master_course = a.master_course
                and c.snapshot_id = a.snapshot_id
            left join
                participation as p
                on p.institution_id = a.institution_id
                and p.ccx_id = a.ccx_id
                and p.subsection_block_id = a.subsection_block_id
            {% if unlinked_attempts is not none %}
                left join
                    unlinked as u
                    on u.institution_id = a.institution_id
                    and u.ccx_id = a.ccx_id
            {% endif %}
            left join
                difficulty as d
                on d.institution_id = a.institution_id
                and d.ccx_id = a.ccx_id
                and d.subsection_block_id = a.subsection_block_id
            left join
                certified as t
                on t.institution_id = a.institution_id
                and t.ccx_id = a.ccx_id
                and t.subsection_block_id = a.subsection_block_id
                and t.snapshot_id = a.snapshot_id
            where a.in_latest_publish
        )
    select
        * except (candidate_sittings),
        if(
            participation_status != 'invalid_window'
            and registered_learners is not null
            and (
                (has_coverage_claim and not has_unresolved_participant)
                or observed_attempted_learners = registered_learners
            ),
            observed_attempted_learners,
            null
        ) as attempted_learners,
        if(
            registered_learners > 0, attempted_learners = registered_learners, null
        ) as delivery_complete
    from evidence
{% endmacro %}
