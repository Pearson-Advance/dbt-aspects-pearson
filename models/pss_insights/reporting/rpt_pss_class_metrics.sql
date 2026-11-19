{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(coalesce(institution_id, ''), ccx_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": pss_reporting_upstream_mvs("class")
            + [
                this.schema ~ ".rpt_pss_learner_progress_mv",
                this.schema ~ ".rpt_pss_class_activity_delivery_mv",
            ],
        },
        tags=["pss_insights"],
    )
}}

{{
    pss_class_metrics(
        ref("int_pss_class_context"),
        ref("rpt_pss_learner_progress"),
        ref("rpt_pss_practice_subdomain_results"),
        ref("int_pss_class_catalog_coverage"),
        ref("rpt_pss_class_activity_delivery"),
        "toDate(now('UTC'))",
    )
}}
