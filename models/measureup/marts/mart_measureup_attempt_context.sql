{{
    config(
        tags=[
            "measureup",
            "measureup_marts",
        ],
    )
}}

{#
    Shared reusable transformation: resolves, for each
    current exam attempt, the exact test version attempted, its LTI launch
    context (if any), and the licensed class that context belongs to (if
    any). Reused by every presentation view descended from an attempt
    (rpt_measureup_attempts, _domain_performance, _subdomain_performance,
    _question_performance) so this join chain is written exactly once.

    Every join here is a left join from a foreign key to its target's
    source_id (or, for institution_ccx, a deduplicated ccx_id — verified
    empirically against stage and prod with no duplicates found, kept
    defensive anyway) — each one is at most 1:1, so this cannot multiply
    attempt rows. has_lti_context and has_licensed_class_context make a
    missing relationship explicit instead of silently dropping the
    attempt — an unresolved LTI or licensed-class link must stay visible
    downstream rather than disappear through an inner join. Compared
    against '', not IS NOT NULL — verified empirically that ClickHouse's
    default join_use_nulls=0 fills an unmatched left join's non-Nullable
    String columns with '', not NULL, so IS NOT NULL is always true
    regardless of whether the join matched. Every column projected
    straight from lti_context or institution_ccx is subject to the same
    default-fill on a miss — expect '', not NULL, on those columns when
    the corresponding has_*_context flag is false. Wrapped in coalesce()
    so the flag stays a deterministic false, not NULL, if join_use_nulls
    is ever set to 1 (ClickHouse's non-default, but the norm on e.g.
    Postgres/Snowflake) — under join_use_nulls=1 an unwrapped
    `x != ''` on a genuinely NULL x evaluates to NULL, not false.

    mup_attempt_configuration (restricted JSON) is deliberately not
    projected here — dashboard-ready views must not expose it.
#}
select
    attempt.source_id as exam_attempt_id,
    attempt.exam_test_id,
    attempt.flow_state,
    attempt.attempt_initiated_datetime,
    attempt.attempt_finished_datetime,
    attempt.attempt_duration,
    attempt.final_score,
    attempt.is_passed,
    attempt.mode,

    exam_test.name as exam_test_name,
    exam_test.product_id as exam_test_product_id,

    coalesce(lti_context.source_id, '') != '' as has_lti_context,
    lti_context.user_id as lti_user_id,
    lti_context.course_id as lti_course_id,

    coalesce(institution_ccx.source_id, '') != '' as has_licensed_class_context,
    institution_ccx.source_id as institution_ccx_id,
    institution_ccx.institution_id,
    institution_ccx.license_id

from {{ ref("int_measureup_exam_test_attempt_current") }} as attempt
left join
    {{ ref("int_measureup_exam_test_current") }} as exam_test
    on attempt.exam_test_id = exam_test.source_id
left join
    {{ ref("int_measureup_lti_launch_context_current") }} as lti_context
    on attempt.lti_launch_event_id = lti_context.source_id
left join
    (
        -- Defensive guard: at most one row per ccx_id for join-fanout
        -- safety. ccx_id uniqueness was verified empirically with zero
        -- duplicates in local, stage and prod, but is collapsed here
        -- anyway rather than assumed to hold at scale. On a tie the row
        -- with the highest source_id wins, a deterministic choice.
        -- This is a plain aggregation over the already-materialized
        -- scalar columns of the _current model — not argMaxMerge over
        -- AggregateFunction state — so it is a single pass, portable
        -- across ClickHouse versions, and consistent with the argMax
        -- idiom used throughout this project. Only the columns this mart
        -- consumes are carried through. src.* is qualified because the
        -- new-analyzer resolves an unqualified argMax(source_id,
        -- source_id) against its own "as source_id" alias and rejects it
        -- as a nested aggregate.
        select
            src.ccx_id as ccx_id,
            max(src.source_id) as source_id,
            argMax(src.institution_id, src.source_id) as institution_id,
            argMax(src.license_id, src.source_id) as license_id
        from {{ ref("int_course_licensing_institution_ccx_current") }} as src
        group by src.ccx_id
    ) as institution_ccx
    on lti_context.course_id = institution_ccx.ccx_id
