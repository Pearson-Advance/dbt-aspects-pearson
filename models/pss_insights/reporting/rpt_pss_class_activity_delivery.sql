{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(coalesce(institution_id, ''), ccx_id, subsection_block_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": pss_reporting_upstream_mvs("delivery")
            + [this.schema ~ ".int_pss_class_catalog_coverage_mv"],
        },
        tags=["pss_insights", "pss_reporting"],
    )
}}

{{ pss_class_activity_delivery() }}
