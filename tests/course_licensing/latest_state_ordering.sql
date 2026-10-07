{#
    Warehouse-level fixture for the shared Course Licensing latest-state
    ordering. The native dbt unit-test layer cannot compare AggregateFunction
    argMaxState values, so this exercises the same ClickHouse argMax ordering
    with plain values. It covers duplicate delivery, out-of-order physical
    arrival, dump-time precedence, tombstones, resurrection, delete tie-breaks,
    and sink_event_id tie-breaking.
#}
with
    source_events as (
        select *
        from
            (
                select
                    'duplicate' as source_id,
                    'same' as payload,
                    'False' as is_deleted,
                    '2026-01-01 00:00:00.000000+00:00' as source_updated_at,
                    '2026-01-01 00:00:01.000000+00:00' as time_last_dumped,
                    toUUID('00000000-0000-0000-0000-000000000001') as sink_event_id
                union all
                select
                    'duplicate',
                    'same',
                    'False',
                    '2026-01-01 00:00:00.000000+00:00',
                    '2026-01-01 00:00:01.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000001')
                union all
                -- Later event is deliberately listed before the older event.
                select
                    'out_of_order',
                    'new',
                    'False',
                    '2026-01-01 00:10:00.000000+00:00',
                    '2026-01-01 00:10:02.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000012')
                union all
                select
                    'out_of_order',
                    'old',
                    'False',
                    '2026-01-01 00:00:00.000000+00:00',
                    '2026-01-01 00:00:02.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000011')
                union all
                -- Equal source timestamps: later dump timestamp must win.
                select
                    'dump_first',
                    'older_dump',
                    'False',
                    '2026-01-01 01:00:00.000000+00:00',
                    '2026-01-01 01:00:01.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000021')
                union all
                select
                    'dump_first',
                    'newer_dump',
                    'False',
                    '2026-01-01 01:00:00.000000+00:00',
                    '2026-01-01 01:00:02.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000022')
                union all
                select
                    'deleted',
                    'live',
                    'False',
                    '2026-01-01 02:00:00.000000+00:00',
                    '2026-01-01 02:00:01.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000031')
                union all
                select
                    'deleted',
                    'tombstone',
                    'True',
                    '2026-01-01 02:05:00.000000+00:00',
                    '2026-01-01 02:05:01.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000032')
                union all
                select
                    'resurrected',
                    'initial',
                    'False',
                    '2026-01-01 03:00:00.000000+00:00',
                    '2026-01-01 03:00:01.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000041')
                union all
                select
                    'resurrected',
                    'deleted',
                    'True',
                    '2026-01-01 03:05:00.000000+00:00',
                    '2026-01-01 03:05:01.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000042')
                union all
                select
                    'resurrected',
                    'restored',
                    'False',
                    '2026-01-01 03:10:00.000000+00:00',
                    '2026-01-01 03:10:01.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000043')
                union all
                -- At identical dump/source timestamps the DELETE must outrank UPSERT.
                select
                    'delete_tie',
                    'upsert',
                    'False',
                    '2026-01-01 04:00:00.000000+00:00',
                    '2026-01-01 04:00:01.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000051')
                union all
                select
                    'delete_tie',
                    'delete',
                    'True',
                    '2026-01-01 04:00:00.000000+00:00',
                    '2026-01-01 04:00:01.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000052')
                union all
                select
                    'event_id_tie',
                    'lower_id',
                    'False',
                    '2026-01-01 05:00:00.000000+00:00',
                    '2026-01-01 05:00:01.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000061')
                union all
                select
                    'event_id_tie',
                    'higher_id',
                    'False',
                    '2026-01-01 05:00:00.000000+00:00',
                    '2026-01-01 05:00:01.000000+00:00',
                    toUUID('00000000-0000-0000-0000-000000000062')
            )
    ),
    resolved as (
        select
            source_id,
            argMax(
                tuple(
                    payload,
                    is_deleted,
                    source_updated_at,
                    time_last_dumped,
                    sink_event_id
                ),
                tuple({{ course_licensing_latest_state_order() }})
            ) as latest
        from source_events
        group by source_id
    ),
    expected as (
        select 'duplicate' as source_id, 'same' as payload, 'False' as is_deleted
        union all
        select 'out_of_order', 'new', 'False'
        union all
        select 'dump_first', 'newer_dump', 'False'
        union all
        select 'deleted', 'tombstone', 'True'
        union all
        select 'resurrected', 'restored', 'False'
        union all
        select 'delete_tie', 'delete', 'True'
        union all
        select 'event_id_tie', 'higher_id', 'False'
    )
select resolved.source_id
from resolved
inner join expected using (source_id)
where
    resolved.latest .1 != expected.payload or resolved.latest .2 != expected.is_deleted
