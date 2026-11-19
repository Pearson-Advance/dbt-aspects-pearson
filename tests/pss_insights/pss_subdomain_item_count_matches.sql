{{ config(tags=["pss_insights"]) }}

{#
    item_subdomain_count equals the item's row count in the map, so splitting
    credit by it never multiplies rows downstream.
#}
select
    master_course,
    problem_block_id,
    count() as row_count,
    any(item_subdomain_count) as declared_count,
    uniqExact(item_subdomain_count) as declared_variants
from {{ ref("int_pss_quiz_item_subdomain_map") }}
group by master_course, problem_block_id
having row_count != declared_count or declared_variants != 1
