{{ config(tags=["pss_scores"]) }}

select ccx_id, actor_id, subsection_block_id, exam_test_id, mode
from {{ ref("rpt_pss_activity_attempts") }}
where attempt_number is not null
group by ccx_id, actor_id, subsection_block_id, exam_test_id, mode
having
    min(attempt_number) != 1
    or max(attempt_number) != count()
    or uniqExact(attempt_number) != count()
