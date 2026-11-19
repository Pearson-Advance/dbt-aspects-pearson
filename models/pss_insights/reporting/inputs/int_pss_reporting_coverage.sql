-- Each row certifies a continuous interval of attempt telemetry for a single
-- published activity. Without a reconciled input, there are no certified dates.
{% if var("pss_activity_coverage_model", none) %}
    select ccx_id, subsection_block_id, coverage_start, coverage_end
    from {{ ref(var("pss_activity_coverage_model")) }}
{% else %}
    select
        cast('' as String) as ccx_id,
        cast('' as String) as subsection_block_id,
        cast(null as Nullable(DateTime64(6, 'UTC'))) as coverage_start,
        cast(null as Nullable(DateTime64(6, 'UTC'))) as coverage_end
    where false
{% endif %}
