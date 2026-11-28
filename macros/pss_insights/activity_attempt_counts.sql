{# Supply UTC SQL timestamp expressions. Count participation at its recorded start. #}
{% macro pss_activity_attempt_counts(period_start, period_end) %}
    {{
        pss_activity_attempt_counts_query(
            ref("rpt_pss_activity_attempts"),
            ref("int_pss_activity_catalog"),
            ref("int_pss_activity_attempt_coverage"),
            period_start,
            period_end,
        )
    }}
{% endmacro %}

{% macro pss_activity_attempt_counts_query(
    attempts, catalog, coverage, period_start, period_end
) %}
    with
        {{ period_start }} as window_start,
        {{ period_end }} as window_end,
        observed as (
            select
                ccx_id,
                subsection_block_id,
                countIf(
                    is_participation = 1 and attempted_at is not null
                ) as dated_participation,
                countIf(is_participation is null or attempted_at is null) as unresolved
            from {{ attempts }}
            where
                (is_participation is null or is_participation = 1)
                and (
                    attempted_at is null
                    or (attempted_at >= window_start and attempted_at < window_end)
                )
            group by ccx_id, subsection_block_id
        ),
        certified as (
            select distinct ccx_id, subsection_block_id, 1 as covers_window
            from {{ coverage }}
            where coverage_start <= window_start and coverage_end >= window_end
        )
    select
        a.institution_id as institution_id,
        a.ccx_id as ccx_id,
        a.subsection_block_id as subsection_block_id,
        ifNull(o.dated_participation, 0) as observed_participation_count,
        multiIf(
            window_start is null or window_end is null or window_start >= window_end,
            'invalid_window',
            ifNull(o.unresolved, 0) > 0,
            'unresolved_attempts',
            ifNull(c.covers_window, 0) = 0,
            'unverified_interval',
            'verified'
        ) as period_status,
        if(
            period_status = 'verified', observed_participation_count, null
        ) as participation_attempt_count
    from {{ catalog }} as a
    left join
        observed as o
        on a.ccx_id = o.ccx_id
        and a.subsection_block_id = o.subsection_block_id
    left join
        certified as c
        on a.ccx_id = c.ccx_id
        and a.subsection_block_id = c.subsection_block_id
    where a.in_latest_publish
{% endmacro %}
