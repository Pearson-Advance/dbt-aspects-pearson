{{ config(tags=["pss_scores"]) }}

select
    master_course,
    activity_type,
    lowerUTF8(trim(flow_state)) as required_state,
    lowerUTF8(trim(mode)) as required_mode,
    any(is_participation) as participation,
    any(is_eligible_practice) as practice,
    any(is_qualifying_final) as final,
    count() = 1
    and notEmpty(required_state)
    and notEmpty(required_mode)
    and participation in (0, 1)
    and practice in (0, 1)
    and final in (0, 1)
    and (practice = 0 or (activity_type = 'Practice Exam' and participation = 1))
    and (
        final = 0 or (activity_type = 'Final Exam' and participation = 1)
    ) as valid_rule
from {{ ref("pss_measureup_attempt_rules") }}
group by master_course, activity_type, required_state, required_mode
