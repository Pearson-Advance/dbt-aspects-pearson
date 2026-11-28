{{ config(schema="pss_restricted", tags=["pss_readiness"]) }}

-- One row per surviving question result. The shared query checks the
-- independent question and attempt catalog paths and detects repeated item IDs.
{{ pss_measureup_question_evidence("Practice Exam") }}
