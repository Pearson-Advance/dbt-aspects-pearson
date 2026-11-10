{{
    config(
        tags=[
            "measureup",
            "measureup_attempts_results_readiness_lti",
            "measureup_attempts",
            "measureup_current",
        ],
    )
}}

select attempt.*, grouped_attempts.attempt_count
from {{ ref("int_measureup_exam_test_attempt_latest_state") }} as attempt
inner join
    (
        select
            argMax(
                src.source_id,
                tuple(
                    parseDateTime64BestEffortOrNull(
                        src.attempt_initiated_datetime, 6, 'UTC'
                    ),
                    toUInt64(src.source_id)
                )
            ) as latest_source_id,
            if(ungrouped_source_id = '', count(), null) as attempt_count
        from {{ ref("int_measureup_exam_test_attempt_latest_state") }} as src
        left join
            {{ ref("int_measureup_lti_launch_context_current") }} as lti_context
            on src.lti_launch_event_id = lti_context.source_id
        where src.is_deleted != 'True'
        group by
            lti_context.user_id,
            lti_context.course_id,
            src.exam_test_id,
            -- Unresolved attempts remain separate instead of sharing a group.
            if(
                coalesce(lti_context.user_id, '') != ''
                and coalesce(lti_context.course_id, '') != ''
                and coalesce(src.exam_test_id, '') != ''
                and parseDateTime64BestEffortOrNull(
                    src.attempt_initiated_datetime, 6, 'UTC'
                )
                is not null,
                '',
                src.source_id
            ) as ungrouped_source_id
    ) as grouped_attempts
    on attempt.source_id = grouped_attempts.latest_source_id
