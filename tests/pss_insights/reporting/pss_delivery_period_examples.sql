{{ config(tags=["pss_reporting", "pss_reporting_fixture"]) }}

{# February counts attempts from February 1 inclusive through March 1 exclusive.
   Test the same query that powers the snapshot and date-selected delivery. #}
with
    class_examples as (
        select
            *,
            'v' as snapshot_id,
            if(ccx_id = 'B', 0, 2) as resolved_registered_learners,
            if(
                ccx_id = 'C', null, resolved_registered_learners
            ) as registered_actor_denominator,
            if(ccx_id = 'C', 'identity_incomplete', 'resolved') as roster_status
        from
        values
            (
                'ccx_id String, institution_id String, master_course String, catalog_status String',
                ('A', 'I', 'M', 'complete'),
                ('B', 'I', 'M', 'complete'),
                ('C', 'I', 'M', 'complete'),
                ('E', 'other', 'M', 'complete')
            )
    ),
    inventory_examples as (
        select
            *,
            if(ccx_id = 'E', 'other', 'I') as institution_id,
            'v' as snapshot_id,
            subsection_block_id as subsection_name,
            1 as section_course_order,
            activity_sequence as subsection_course_order
        from
        values
            (
                'ccx_id String, master_course String, subsection_block_id String, activity_type String, activity_sequence UInt64, in_latest_publish Bool',
                ('A', 'M', 'never', 'Quiz', 1, 1),
                ('A', 'M', 'unlinked', 'Lab', 7, 1),
                ('A', 'M', 'missing', 'Lab', 2, 1),
                ('A', 'M', 'partial', 'Quiz', 3, 1),
                ('A', 'M', 'all', 'Lab', 4, 1),
                ('A', 'M', 'removed', 'Lab', 5, 0),
                ('B', 'M', 'empty', 'Quiz', 1, 1),
                ('C', 'M', 'partial-roster', 'Lab', 1, 1),
                ('A', 'M', 'unknown', 'Quiz', 5, 1),
                ('A', 'M', 'timeless', 'Lab', 6, 1),
                ('E', 'M', 'hidden', 'Lab', 1, 1)
            )
    ),
    member_examples as (
        select *
        from
        values
            (
                'ccx_id String, actor_id String',
                ('A', '1'),
                ('A', '1'),
                ('A', '2'),
                ('C', '1'),
                ('C', '2'),
                ('E', '1')
            )
    ),
    attempt_examples as (
        select
            *,
            if(ccx_id = 'E', 'other', 'I') as institution_id,
            multiIf(
                subsection_block_id = 'unknown',
                null,
                ifNull(score_pct = 125, false),
                0,
                1
            ) as is_participation
        from
        values
            (
                'ccx_id String, subsection_block_id String, actor_id String, attempted_at Nullable(DateTime(\'UTC\')), score_pct Nullable(Float64)',
                ('A', 'never', '1', '2026-03-01', 70),
                ('A', 'never', '3', '2026-02-10', 90),
                ('A', 'missing', '1', '2026-02-01', 0),
                ('A', 'partial', '1', '2026-02-01', 0),
                ('A', 'partial', '1', '2026-02-15', 60),
                ('A', 'partial', '2', '2026-01-31', 90),
                ('A', 'all', '1', '2026-02-15', null),
                ('A', 'all', '2', '2026-02-16', 80),
                ('A', 'all', '2', '2026-02-17', 125),
                ('A', 'unknown', '2', '2026-02-03', 80),
                ('A', 'timeless', '1', null, 50),
                ('C', 'partial-roster', '1', '2026-02-08', 50),
                ('C', 'partial-roster', '2', '2026-02-12', 100)
            )
    ),
    score_examples as (
        select *, 'I' as institution_id
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
        select
            *,
            'I' as institution_id,
            'v' as snapshot_id,
            coverage_end is null and coverage_start is null as full_history,
            toDateTime('2026-03-02', 'UTC') as verified_through
        from
        values
            (
                'ccx_id String, subsection_block_id String, coverage_start Nullable(DateTime(\'UTC\')), coverage_end Nullable(DateTime(\'UTC\'))',
                ('A', 'never', '2026-02-01', '2026-03-01'),
                ('A', 'unlinked', '2026-02-01', '2026-03-01'),
                ('A', 'partial', null, null),
                ('A', 'missing', '2026-02-05', null),
                ('B', 'empty', null, null),
                ('A', 'unknown', null, null),
                ('A', 'timeless', null, null),
                ('C', 'partial-roster', null, null)
            )
    ),
    unlinked_rows as (
        select
            'I' as institution_id,
            'A' as ccx_id,
            '1' as actor_id,
            toNullable('unlinked') as subsection_block_id,
            toDateTime('2026-02-20', 'UTC') as attempted_at
        union all
        select 'I', 'A', '2', null, toDateTime('2026-03-01', 'UTC')
        union all
        select 'I', 'A', '1', 'partial', toDateTime('2026-02-10', 'UTC')
    ),
    access_rows as (
        select *
        from
        values
            (
                'institution_id String,ccx_id String',
                ('I', 'A'),
                ('I', 'A'),
                ('I', 'B'),
                ('I', 'C'),
                ('I', 'E')
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
                "access_rows",
                "unlinked_rows",
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
                'ccx_id String, subsection_block_id String, activity_sequence UInt64, registered_learners Nullable(UInt64), observed_attempted_learners UInt64, attempted_learners Nullable(UInt64), delivery_complete Nullable(UInt8), avg_score_pct Nullable(Float64), avg_attempt_score_pct Nullable(Float64)',
                ('A', 'never', 1, 2, 0, 0, 0, null, null),
                ('A', 'unlinked', 7, 2, 0, null, null, null, null),
                ('A', 'missing', 2, 2, 1, null, null, null, 0),
                ('A', 'partial', 3, 2, 1, 1, 0, 40, 30),
                ('A', 'all', 4, 2, 2, 2, 1, null, 80),
                ('B', 'empty', 1, 0, 0, 0, null, null, null),
                ('A', 'unknown', 5, 2, 0, null, null, null, null),
                ('A', 'timeless', 6, 2, 0, null, null, null, null),
                ('C', 'partial-roster', 1, null, 2, null, null, null, 75)
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
