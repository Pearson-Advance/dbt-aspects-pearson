{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(ccx_id, actor_id, subsection_block_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [
                target.schema ~ ".rpt_pss_activity_attempts_mv",
                target.schema ~ ".dim_course_blocks_mv",
            ],
            "depends_on_validation": true,
        },
        tags=["pss_scores"],
    )
}}

{% set rules = var("pss_current_score_rules", {}) %}
{% if rules is not mapping %}
    {{
        exceptions.raise_compiler_error(
            "pss_current_score_rules must map exact master course keys to activity type and rule"
        )
    }}
{% endif %}
{% for master, activities in rules.items() %}
    {% if master is not string or not master.startswith(
        "course-v1:"
    ) or activities is not mapping %}
        {{
            exceptions.raise_compiler_error(
                "Invalid master key or activity configuration in pss_current_score_rules"
            )
        }}
    {% endif %}
    {% for activity, rule in activities.items() %}
        {% if activity not in [
            "Quiz",
            "Lab",
            "Practice Exam",
            "Final Exam",
        ] or rule not in ["lms_subsection", "measureup_latest_finished"] %}
            {{
                exceptions.raise_compiler_error(
                    "Unknown PSS activity type or current score rule"
                )
            }}
        {% endif %}
    {% endfor %}
{% endfor %}
{% set encoded = tojson(rules) | replace("\\", "\\\\") | replace("'", "''") %}

with
    candidates as (
        select
            c.institution_id as institution_id,
            c.master_course as master_course,
            assumeNotNull(c.ccx_id) as ccx_id,
            assumeNotNull(e.actor_id) as actor_id,
            assumeNotNull(c.subsection_block_id) as subsection_block_id,
            c.activity_type as activity_type,
            e.provider as provider,
            e.source_id as source_id,
            e.observed_at as observed_at,
            e.score_pct as candidate_pct,
            e.score_status as candidate_status,
            e.is_finished as is_finished,
            ifNull(
                nullIf(
                    JSONExtractString(
                        '{{ encoded }}', c.master_course, c.activity_type
                    ),
                    ''
                ),
                'agreement'
            ) as selection_rule,
            maxIf(e.observed_at, e.provider = 'measureup' and e.is_finished) over (
                partition by c.ccx_id, e.actor_id, c.subsection_block_id
            ) as latest_finished
        from {{ ref("int_pss_activity_score_candidates") }} as e
        inner join
            {{ ref("int_pss_activity_catalog") }} as c
            on c.ccx_id = e.ccx_id
            and c.subsection_block_id = e.subsection_block_id
        where c.in_latest_publish
    ),
    selected as (
        select
            *,
            multiIf(
                selection_rule = 'lms_subsection',
                provider = 'native',
                selection_rule = 'measureup_latest_finished',
                provider = 'measureup'
                and is_finished
                and observed_at = latest_finished,
                true
            ) as considered
        from candidates
    ),
    grouped as (
        select
            ccx_id,
            actor_id,
            subsection_block_id,
            institution_id,
            master_course,
            activity_type,
            selection_rule,
            countIf(considered) as selected_count,
            countIf(
                considered and (candidate_status != 'observed' or candidate_pct is null)
            ) as unavailable_count,
            maxOrNullIf(
                candidate_pct, considered and candidate_status = 'observed'
            ) as highest_pct,
            minOrNullIf(
                candidate_pct, considered and candidate_status = 'observed'
            ) as agreed_pct,
            arraySort(
                groupUniqArrayIf(
                    candidate_status, considered and candidate_status != 'observed'
                )
            ) as evidence_gaps,
            arraySort(groupUniqArrayIf(provider, considered)) as selected_sources,
            arraySort(
                groupUniqArrayIf(tuple(provider, source_id), considered)
            ) as selected_source_ids,
            maxOrNullIf(observed_at, considered) as observed_at,
            uniqExactIf(source_id, provider = 'native') as native_grade_count,
            uniqExactIf(source_id, provider = 'measureup') as provider_attempt_count
        from selected
        group by
            ccx_id,
            actor_id,
            subsection_block_id,
            institution_id,
            master_course,
            activity_type,
            selection_rule
    )
select
    ccx_id,
    actor_id,
    subsection_block_id,
    institution_id,
    master_course,
    activity_type,
    selection_rule,
    multiIf(
        selected_count = 0,
        'configured_source_unavailable',
        unavailable_count > 0,
        if(length(evidence_gaps) = 1, evidence_gaps[1], 'incomplete_score_evidence'),
        highest_pct - agreed_pct > 1e-12
        and selection_rule = 'agreement'
        and provider_attempt_count > 0,
        'score_rule_required',
        highest_pct - agreed_pct > 1e-12,
        'conflicting_latest_scores',
        'observed'
    ) as score_status,
    if(score_status = 'observed', agreed_pct, null) as score_pct,
    if(
        score_status = 'observed', arrayStringConcat(selected_sources, '+'), null
    ) as score_source,
    observed_at as score_observed_at,
    selected_source_ids,
    native_grade_count,
    provider_attempt_count,
    evidence_gaps
from grouped
