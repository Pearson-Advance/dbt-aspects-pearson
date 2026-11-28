{% macro pss_valid_reporting_attestations(relation) %}
    select
        *,
        parseDateTime64BestEffortOrNull(verified_at, 6, 'UTC') as checked_at,
        parseDateTime64BestEffortOrNull(valid_until, 6, 'UTC') as expires_at
    from {{ relation }}
    where
        notEmpty(trim(ifNull(evidence_ref, '')))
        and notEmpty(ifNull(institution_id, ''))
        and notEmpty(ifNull(ccx_id, ''))
        and notEmpty(ifNull(snapshot_id, ''))
        and checked_at <= now64(6, 'UTC')
        and now64(6, 'UTC') < expires_at
{% endmacro %}
