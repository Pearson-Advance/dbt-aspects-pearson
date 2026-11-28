{{ config(tags=["pss_scores"]) }}
with
    catalog as (
        select
            'org' as institution_id,
            'class' as ccx_id,
            name as subsection_block_id,
            1 as in_latest_publish
        from
        values
            (
                'name String',
                'dated',
                'undated',
                'unreviewed',
                'unqualified',
                'unverified'
            )
    ),
    attempts as (
        select
            'class' as ccx_id,
            name as subsection_block_id,
            eligible as is_participation,
            parseDateTime64BestEffortOrNull(at, 6, 'UTC') as attempted_at
        from
        values
            (
                'name String,eligible Nullable(UInt8),at String',
                ('dated', 1, '2025-11-01T10:00:00Z'),
                ('dated', 1, '2025-11-01T11:00:00Z'),
                ('undated', 1, ''),
                ('unreviewed', NULL, '2025-11-01T10:30:00Z'),
                ('unqualified', 0, '')
            )
    ),
    coverage as (
        select
            ccx_id,
            subsection_block_id,
            toDateTime64('2025-11-01 10:00:00', 6, 'UTC') as coverage_start,
            toDateTime64('2025-11-01 11:00:00', 6, 'UTC') as coverage_end
        from catalog
        where subsection_block_id != 'unverified'
    ),
    results as (
        {{
            pss_activity_attempt_counts_query(
                "attempts",
                "catalog",
                "coverage",
                "toDateTime64('2025-11-01 10:00:00',6,'UTC')",
                "toDateTime64('2025-11-01 11:00:00',6,'UTC')",
            )
        }}
    )
select 1
where
    (
        select
            count() = 5
            and countIf(
                subsection_block_id = 'dated' and participation_attempt_count = 1
            )
            = 1
            and countIf(
                subsection_block_id = 'unqualified' and participation_attempt_count = 0
            )
            = 1
            and countIf(
                subsection_block_id in ('unreviewed', 'undated')
                and period_status = 'unresolved_attempts'
                and participation_attempt_count is null
            )
            = 2
            and countIf(
                subsection_block_id = 'unverified'
                and period_status = 'unverified_interval'
                and participation_attempt_count is null
            )
            = 1
        from results
    )
    != 1
