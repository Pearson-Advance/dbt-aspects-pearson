{{ config(tags=["pss_insights"]) }}

{# No PSS output exposes the restricted course-settings JSON. #}
select table, name
from system.columns
where
    database = '{{ target.schema }}'
    and (table like 'int_pss_%' or table = 'pss_grading_policy')
    and positionCaseInsensitive(name, 'settings') > 0
