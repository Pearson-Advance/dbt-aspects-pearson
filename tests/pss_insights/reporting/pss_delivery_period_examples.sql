{{ config(tags=["pss_insights", "pss_reporting", "pss_reporting_fixture"]) }}

{# February counts attempts from February 1 inclusive through March 1 exclusive.
   Test the same query that powers the snapshot and date-selected delivery. #}
with
    class_examples as (
        select *
        from
        values
            (
                'ccx_id String, institution_id String, master_course String, catalog_status String',
                ('A', 'I', 'M', 'complete'),
                ('B', 'I', 'M', 'complete')
            )
    ),
    inventory_examples as (
        select
            *,
            subsection_block_id as subsection_name,
            1 as section_course_order,
            activity_sequence as subsection_course_order
        from
        values
            (
                'ccx_id String, master_course String, subsection_block_id String, activity_type String, activity_sequence UInt64, in_latest_publish Bool',
                ('A', 'M', 'never', 'Quiz', 1, 1),
                ('A', 'M', 'missing', 'Lab', 2, 1),
                ('A', 'M', 'partial', 'Quiz', 3, 1),
                ('A', 'M', 'all', 'Lab', 4, 1),
                ('A', 'M', 'removed', 'Lab', 5, 0),
                ('B', 'M', 'empty', 'Quiz', 1, 1)
            )
    ),
    member_examples as (
        select *
        from
        values ('ccx_id String, actor_id String', ('A', '1'), ('A', '1'), ('A', '2'))
    ),
    attempt_examples as (
        select *
        from
        values
            (
                'ccx_id String, subsection_block_id String, actor_id String, attempted_at DateTime(\'UTC\'), score_pct Nullable(Float64)',
                ('A', 'never', '1', '2026-03-01', 70),
                ('A', 'never', '3', '2026-02-10', 90),
                ('A', 'missing', '1', '2026-02-01', 0),
                ('A', 'partial', '1', '2026-02-01', 0),
                ('A', 'partial', '1', '2026-02-15', 60),
                ('A', 'partial', '2', '2026-01-31', 90),
                ('A', 'all', '1', '2026-02-15', null),
                ('A', 'all', '2', '2026-02-16', 80),
                ('A', 'all', '2', '2026-02-17', 125)
            )
    ),
    score_examples as (
        select *
        from
        values
            (
                'ccx_id String, subsection_block_id String, actor_id String, score_pct Nullable(Float64)',
                ('A', 'partial', '1', 0),
                ('A', 'partial', '2', 80),
                ('A', 'partial', '3', 100),
                ('A', 'all', '1', -5)
            )
    ),
    coverage_examples as (
        select *
        from
        values
            (
                'ccx_id String, subsection_block_id String, coverage_start Nullable(DateTime(\'UTC\')), coverage_end Nullable(DateTime(\'UTC\'))',
                ('A', 'never', '2026-02-01', '2026-03-01'),
                ('A', 'partial', null, null),
                ('A', 'missing', '2026-02-05', null),
                ('B', 'empty', null, null)
            )
    ),
    period_results as (
        {{
            pss_activity_delivery_query(
                "class_examples",
                "inventory_examples",
                "member_examples",
                "attempt_examples",
                "score_examples",
                "coverage_examples",
                "toDateTime('2026-02-01', 'UTC')",
                "toDateTime('2026-03-01', 'UTC')",
            )
        }}
    ),
    actual as (
        select
            ccx_id,
            subsection_block_id,
            activity_sequence,
            registered_learners,
            observed_attempted_learners,
            attempted_learners,
            delivery_complete,
            avg_score_pct,
            avg_attempt_score_pct
        from period_results
    ),
    expected as (
        select *
        from
        values
            (
                'ccx_id String, subsection_block_id String, activity_sequence UInt64, registered_learners UInt64, observed_attempted_learners UInt64, attempted_learners Nullable(UInt64), delivery_complete Nullable(UInt8), avg_score_pct Nullable(Float64), avg_attempt_score_pct Nullable(Float64)',
                ('A', 'never', 1, 2, 0, 0, 0, null, null),
                ('A', 'missing', 2, 2, 1, null, null, null, 0),
                ('A', 'partial', 3, 2, 1, 1, 0, 40, 30),
                ('A', 'all', 4, 2, 2, 2, 1, null, 80),
                ('B', 'empty', 1, 0, 0, 0, null, null, null)
            )
    )
select *
from
    (
        select *
        from actual
        except distinct
        select *
        from expected
    )
union all
select *
from
    (
        select *
        from expected
        except distinct
        select *
        from actual
    )
