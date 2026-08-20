# Custom model layout

Keep Pearson-owned transformations separated by business domain:

- `course_licensing/`: institutions, licenses, classes, enrollments, courses, and instructor assignments.
- `measureup/`: MeasureUp test catalog, attempts, domains, subdomains, and question hierarchy.
- `shared/`: reusable intermediate models that are genuinely shared by more than one domain.

## Model contract

Every production model should include:

1. A documented grain and primary key.
2. An explicit ClickHouse materialization configuration when it is not a normal view.
3. Deterministic current-state logic based on the newest sink record per business key.
4. Explicit deletion/tombstone handling where the sink emits deleted records.
5. `schema.yml` documentation and data tests for uniqueness, non-null keys, accepted values, and relationships.
6. Unit tests for deduplication, late-arriving records, deleted records, and tie-breaking behavior.

Prefer `ref()` for upstream `aspects-dbt` models. Use `source()` only when the transformation must consume a raw event-sink or xAPI table directly. Do not hard-code database names; use the same environment-variable conventions as Aspects.

Materialized views should opt in within the SQL model, for example:

```sql
{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("ReplacingMergeTree()"),
        primary_key="(entity_id)",
        order_by="(entity_id)",
    )
}}
```

The actual engine, ordering key, partitioning, and replacement/version column must be selected from the model's update semantics rather than copied mechanically.
