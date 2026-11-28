{{ config(schema="pss_restricted", tags=["pss_scores"]) }}

select
    exam_attempt_id,
    lti_launch_event_id,
    nullIf(ccx_id, '') as ccx_id,
    subsection_block_id,
    resolved_link_status as issue
from {{ ref("int_pss_measureup_attempt_attribution") }}
where resolved_link_status != 'linked'
union all
select
    null as exam_attempt_id,
    l.source_id as lti_launch_event_id,
    c.ccx_id as ccx_id,
    null as subsection_block_id,
    'launch_without_attempt' as issue
from {{ ref("int_measureup_lti_launch_context_current") }} as l
inner join {{ ref("int_pss_class_scope") }} as c on c.ccx_id = l.course_id
left anti join
    {{ ref("int_measureup_exam_test_attempt_current") }} as a
    on a.lti_launch_event_id = l.source_id
