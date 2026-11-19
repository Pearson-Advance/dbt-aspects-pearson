{{ config(tags=["pss_insights"]) }}

{#
    activity_sequence is dense 1..n per class over published activities and
    null for activities no longer in the latest publish.
#}
select ccx_id, 'published sequence not dense 1..n' as problem
from {{ ref("int_pss_activity_catalog") }}
where in_latest_publish
group by ccx_id
having
    countIf(activity_sequence is null) > 0
    or min(activity_sequence) != 1
    or max(activity_sequence) != count()
    or uniqExact(activity_sequence) != count()
union all
select ccx_id, 'unpublished activity with a sequence'
from {{ ref("int_pss_activity_catalog") }}
where not in_latest_publish and activity_sequence is not null
