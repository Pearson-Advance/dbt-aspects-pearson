with
    cases as (
        select
            'ninety_minutes' as case_name,
            cast('2026-01-01T10:00:00Z' as Nullable(String)) as start_value,
            cast('01:30:00' as Nullable(String)) as duration_value,
            cast(toDecimal64(5400, 6) as Nullable(Decimal64(6))) as expected_seconds,
            cast(
                parseDateTime64BestEffortOrNull(
                    '2026-01-01T11:30:00Z', 6, 'UTC'
                ) as Nullable(DateTime64(6, 'UTC'))
            ) as expected_end

        union all

        select
            'cross_midnight',
            '2026-01-01T23:45:00Z',
            '00:30:00',
            toDecimal64(1800, 6),
            parseDateTime64BestEffortOrNull('2026-01-02T00:15:00Z', 6, 'UTC')

        union all

        select
            'zero_duration',
            '2026-01-01T10:00:00Z',
            '00:00:00',
            toDecimal64(0, 6),
            parseDateTime64BestEffortOrNull('2026-01-01T10:00:00Z', 6, 'UTC')

        union all

        select
            'fractional_duration',
            '2026-01-01T10:00:00Z',
            '00:00:00.500000',
            toDecimal64('0.500000', 6),
            parseDateTime64BestEffortOrNull('2026-01-01T10:00:00.500000Z', 6, 'UTC')

        union all

        select
            'longer_than_day',
            '2026-01-01T10:00:00Z',
            '25:30:00',
            toDecimal64(91800, 6),
            parseDateTime64BestEffortOrNull('2026-01-02T11:30:00Z', 6, 'UTC')

        union all

        select
            'malformed_duration',
            '2026-01-01T10:00:00Z',
            'not-a-duration',
            cast(null as Nullable(Decimal64(6))),
            cast(null as Nullable(DateTime64(6, 'UTC')))

        union all

        select
            'negative_duration',
            '2026-01-01T10:00:00Z',
            '-00:30:00',
            cast(null as Nullable(Decimal64(6))),
            cast(null as Nullable(DateTime64(6, 'UTC')))

        union all

        select
            'null_duration',
            '2026-01-01T10:00:00Z',
            cast(null as Nullable(String)),
            cast(null as Nullable(Decimal64(6))),
            cast(null as Nullable(DateTime64(6, 'UTC')))

        union all

        select
            'null_start',
            cast(null as Nullable(String)),
            '00:30:00',
            toDecimal64(1800, 6),
            cast(null as Nullable(DateTime64(6, 'UTC')))
    ),

    normalized as (
        select
            case_name,
            parseDateTime64BestEffortOrNull(start_value, 6, 'UTC') as start_at,
            {{ measureup_attempt_duration_seconds("duration_value") }}
            as actual_seconds,
            expected_seconds,
            expected_end
        from cases
    ),

    calculated as (
        select
            case_name,
            actual_seconds,
            expected_seconds,
            if(
                start_at is not null
                and actual_seconds is not null
                and actual_seconds >= 0,
                addMicroseconds(start_at, toInt64(actual_seconds * 1000000)),
                cast(null as Nullable(DateTime64(6, 'UTC')))
            ) as actual_end,
            expected_end
        from normalized
    )

select *
from calculated
where
    not coalesce(
        actual_seconds = expected_seconds,
        isNull(actual_seconds) and isNull(expected_seconds)
    )
    or not coalesce(
        actual_end = expected_end, isNull(actual_end) and isNull(expected_end)
    )
