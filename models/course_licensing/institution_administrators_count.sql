{{ 
    config(
        materialized="materialized_view",
        schema=env_var("ASPECTS_EVENT_SINK_DATABASE", "event_sink"),
        post_hook="OPTIMIZE TABLE {{ this }} {{ on_cluster }} FINAL"
    ) 
}}

SELECT
    institution_id,
    count(id) as institution_administrator_count
FROM {{ source("event_sink", "course_licensing_institution_administrator") }} institution_administrator
JOIN
    (
        SELECT 
            id, 
            max(time_last_dumped) as max_time_last_dumped
        FROM {{ source("event_sink", "course_licensing_institution_administrator") }}
        GROUP BY id
    ) latest_institution_administrators
    ON institution_administrator.id = latest_institution_administrators.id
    AND institution_administrator.time_last_dumped = latest_institution_administrators.max_time_last_dumped
GROUP BY institution_id
