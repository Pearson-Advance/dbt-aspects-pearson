{{ config(schema="pss_restricted", tags=["pss_readiness", "pss_quiz_weakness"]) }}

-- Optional independently verified sitting and selected result. The current xAPI
-- export only identifies the individual problem evaluation. No context is inferred.
{% if var("pss_native_quiz_context_model", none) %}
    select
        ccx_id,
        actor_id,
        source_result_id,
        attempt_key,
        attempted_at,
        subsection_block_id,
        problem_block_id,
        snapshot_id,
        evidence_ref,
        mapping_ref,
        objectives
    from {{ ref(var("pss_native_quiz_context_model")) }}
{% else %}
    select
        '' as ccx_id,
        '' as actor_id,
        '' as source_result_id,
        '' as attempt_key,
        toDateTime64(0, 6, 'UTC') as attempted_at,
        '' as subsection_block_id,
        '' as problem_block_id,
        '' as snapshot_id,
        '' as evidence_ref,
        '' as mapping_ref,
        cast([], 'Array(Tuple(UInt64, UInt64, UInt64))') as objectives
    where false
{% endif %}
