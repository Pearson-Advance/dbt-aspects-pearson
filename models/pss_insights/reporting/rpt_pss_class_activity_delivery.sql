{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(coalesce(institution_id, ''), ccx_id, subsection_block_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [
                target.schema
                ~ "_pss_restricted.int_pss_reporting_unlinked_attempts_mv",
                target.schema ~ ".rpt_pss_activity_attempts_mv",
                target.schema ~ ".rpt_pss_activity_scores_current_mv",
                target.schema ~ ".int_pss_class_catalog_coverage_mv",
            ],
            "depends_on_validation": true,
        },
        tags=["pss_reporting"],
    )
}}

{{ pss_class_activity_delivery() }}
