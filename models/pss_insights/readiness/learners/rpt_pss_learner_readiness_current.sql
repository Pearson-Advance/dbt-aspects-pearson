{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(institution_id,ccx_id,actor_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [
                target.schema ~ ".int_pss_readiness_evidence_mv",
                target.schema ~ ".dim_course_blocks_mv",
            ],
            "depends_on_validation": true,
        },
        tags=["pss_readiness"],
    )
}}
{{ pss_readiness_query(none, none, none) }}
