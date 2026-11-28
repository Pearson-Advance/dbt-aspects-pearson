{{
    config(
        materialized="materialized_view",
        schema="pss_restricted",
        engine=aspects.get_engine("MergeTree()"),
        order_by="(source_result_id)",
        refreshable={
            "interval": "EVERY 4 HOUR",
            "depends_on": [target.schema ~ ".rpt_pss_activity_attempts_mv"],
        },
        tags=["pss_scores", "pss_items"],
    )
}}

{{ pss_measureup_question_evidence("Quiz") }}
