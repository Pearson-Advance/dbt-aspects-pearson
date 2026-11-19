{# Bounds are SQL expressions (inclusive start and exclusive end, UTC).
   An omitted bound requires coverage certified to that end of the history. #}
{% macro pss_class_activity_delivery(period_start=none, period_end=none) %}
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
    period_end=none
) %}
    with
        eligible_members as (select distinct ccx_id, actor_id from {{ roster }}),
        roster_sizes as (
            select ccx_id, count() as registered_learners
            from eligible_members
            group by ccx_id
        ),
        participation as (
            select
                a.ccx_id as ccx_id,
                a.subsection_block_id as subsection_block_id,
                uniqExact(a.actor_id) as observed_attempted_learners,
                avgOrNullIf(
                    a.score_pct, a.score_pct between 0 and 100
                ) as avg_attempt_score_pct
            from {{ attempts }} as a
            inner join
                eligible_members as r on r.ccx_id = a.ccx_id and r.actor_id = a.actor_id
            where
                true
                {% if period_start is not none %}
                    and a.attempted_at >= {{ period_start }}
                {% endif %}
                {% if period_end is not none %}
                    and a.attempted_at < {{ period_end }}
                {% endif %}
            group by a.ccx_id, a.subsection_block_id
        ),
        difficulty as (
            select
                s.ccx_id as ccx_id,
                s.subsection_block_id as subsection_block_id,
                avgOrNullIf(s.score_pct, s.score_pct between 0 and 100) as avg_score_pct
            from {{ scores }} as s
            inner join
                eligible_members as r on r.ccx_id = s.ccx_id and r.actor_id = s.actor_id
            group by s.ccx_id, s.subsection_block_id
        ),
        certified as (
            select distinct ccx_id, subsection_block_id, 1 as is_certified
            from {{ telemetry }}
            where
                (
                    coverage_start is null
                    {% if period_start is not none %}
                        or coverage_start <= {{ period_start }}
                    {% endif %}
                )
                and (
                    coverage_end is null
                    {% if period_end is not none %}
                        or coverage_end >= {{ period_end }}
                    {% endif %}
                )
        ),
        evidence as (
            select
                c.ccx_id as ccx_id,
                c.institution_id as institution_id,
                c.master_course as master_course,
                a.subsection_block_id as subsection_block_id,
                a.subsection_name as subsection_name,
                a.activity_type as activity_type,
                a.section_course_order as section_course_order,
                a.subsection_course_order as subsection_course_order,
                a.activity_sequence as activity_sequence,
                c.catalog_status as catalog_status,
                coalesce(r.registered_learners, 0) as registered_learners,
                coalesce(
                    p.observed_attempted_learners, 0
                ) as observed_attempted_learners,
                coalesce(t.is_certified, 0) as telemetry_complete,
                d.avg_score_pct as avg_score_pct,
                p.avg_attempt_score_pct as avg_attempt_score_pct
            from {{ catalog }} as a
            inner join
                {{ classes }} as c
                on c.ccx_id = a.ccx_id
                and c.master_course = a.master_course
            left join roster_sizes as r on r.ccx_id = a.ccx_id
            left join
                participation as p
                on p.ccx_id = a.ccx_id
                and p.subsection_block_id = a.subsection_block_id
            left join
                difficulty as d
                on d.ccx_id = a.ccx_id
                and d.subsection_block_id = a.subsection_block_id
            left join
                certified as t
                on t.ccx_id = a.ccx_id
                and t.subsection_block_id = a.subsection_block_id
            where a.in_latest_publish
        )
    select
        *,
        if(
            telemetry_complete or observed_attempted_learners = registered_learners,
            observed_attempted_learners,
            null
        ) as attempted_learners,
        if(
            registered_learners > 0, attempted_learners = registered_learners, null
        ) as delivery_complete
    from evidence
{% endmacro %}
