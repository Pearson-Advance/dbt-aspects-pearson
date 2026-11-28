{{ config(tags=["pss_insights", "pss_insights_subdomains"]) }}

-- The Studio snapshot and Aspects course structure are delivered independently.
-- Compare all problem blocks, including ungraded children, against the
-- collector's count before using the structure to attribute Quiz items.
with
    structure as (
        select
            course_key as master_course,
            if(block_type = 'subsection', block_id, subsection_block_id) as parent_key,
            countIf(block_type = 'subsection') as observed_parent_rows,
            uniqExactIf(block_id, block_type = 'problem') as observed_problem_count
        from {{ ref("dim_course_blocks") }}
        where
            in_latest_publish
            and block_type in ('subsection', 'problem')
            and course_key
            in (select master_course from {{ ref("int_pss_master_course_scope") }})
        group by course_key, parent_key
    )
select
    q.master_course as master_course,
    q.subsection_block_id as subsection_block_id,
    q.snapshot_id as snapshot_id,
    q.problem_count as expected_problem_count,
    ifNull(s.observed_problem_count, 0) as observed_problem_count,
    multiIf(
        q.problem_count is null,
        'missing_expected_count',
        ifNull(s.observed_parent_rows, 0) = 0,
        'missing_subsection',
        q.problem_count != observed_problem_count,
        'problem_count_mismatch',
        'counts_match'
    ) as structure_status
from {{ ref("int_pss_activity_type_coverage") }} as q
left join
    structure as s
    on q.master_course = s.master_course
    and q.subsection_block_id = s.parent_key
where
    q.in_latest_publish
    and q.coverage_status = 'classified'
    and q.activity_type = 'Quiz'
