{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(coalesce(institution_id, ''), ccx_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [
                target.schema
                ~ "_pss_restricted.int_pss_reporting_unlinked_attempts_mv",
                target.schema ~ ".rpt_pss_learner_progress_mv",
                target.schema ~ ".rpt_pss_class_activity_delivery_mv",
                target.schema ~ ".rpt_pss_practice_subdomain_results_mv",
                target.schema ~ ".rpt_pss_learner_practice_weaknesses_current_mv",
                target.schema ~ ".int_pss_class_catalog_coverage_mv",
            ],
            "depends_on_validation": true,
        },
        tags=["pss_reporting"],
    )
}}

{{
    pss_class_metrics(
        ref("int_pss_class_context"),
        ref("rpt_pss_learner_progress"),
        ref("rpt_pss_practice_subdomain_results"),
        ref("int_pss_class_catalog_coverage"),
        ref("rpt_pss_class_activity_delivery"),
        "toDate(now(), '" ~ var("pss_reporting_timezone", "UTC") ~ "')",
        ref("rpt_pss_learner_practice_weaknesses_current"),
        ref("int_pss_readiness_history_coverage"),
        ref("int_pss_reporting_unlinked_attempts"),
    )
}}
