{#
    Cross-checks a MeasureUp _current model against an independently
    computed "latest event per source_id" over the raw source, using
    row_number() instead of the model's own argMaxState pipeline. The two
    implementations agreeing is the closest thing to a unit test we can get
    for the argMaxState layer, which ClickHouse cannot deserialize for dbt's
    native unit-test comparison (same limitation documented on
    course_licensing_current_matches_latest_event).

    is_deleted is compared against the sink's own 'True'/'False' string
    values, not cast with toUInt8() — that cast fails on this column
    (Cannot parse string 'False' as UInt8), since is_deleted is a String,
    not a number, in every MeasureUp raw sink table.
#}
{% test measureup_current_matches_latest_event(model, source_name, source_table) %}

    with
        ranked_source as (
            select
                source_id,
                is_deleted,
                source_updated_at,
                time_last_dumped,
                sink_event_id,
                row_number() over (
                    partition by source_id
                    order by ({{ measureup_latest_state_order() }}) desc
                ) as event_rank
            from {{ source(source_name, source_table) }}
            where
                source_id is not null
                and time_last_dumped is not null
                and source_updated_at is not null
                and sink_event_id is not null
        ),

        expected_current as (
            select source_id, source_updated_at, time_last_dumped, sink_event_id
            from ranked_source
            where event_rank = 1 and is_deleted != 'True'
        ),

        actual_current as (
            select source_id, source_updated_at, time_last_dumped, sink_event_id
            from {{ model }}
        )

    select coalesce(expected_current.source_id, actual_current.source_id) as source_id
    from expected_current
    full outer join actual_current using (source_id)
    where
        expected_current.source_id is null
        or actual_current.source_id is null
        or expected_current.time_last_dumped != actual_current.time_last_dumped
        or expected_current.source_updated_at != actual_current.source_updated_at
        or expected_current.sink_event_id != actual_current.sink_event_id

{% endtest %}
