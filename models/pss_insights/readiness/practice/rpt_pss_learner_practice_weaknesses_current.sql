{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(ifNull(institution_id, ''), ifNull(ccx_id, ''), ifNull(actor_id, ''))",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [
                target.schema ~ ".rpt_pss_practice_subdomain_results_mv",
                target.schema ~ ".rpt_pss_activity_attempts_mv",
            ],
            "depends_on_validation": true,
        },
        tags=["pss_readiness"],
    )
}}

{{ pss_practice_weakness_ranking(ref("int_pss_class_scope")) }}
