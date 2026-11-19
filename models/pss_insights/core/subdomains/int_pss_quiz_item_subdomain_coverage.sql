{{ config(tags=["pss_insights", "pss_insights_subdomains"]) }}

{#
    Graded problems in the latest publish of each in-scope master course, with
    and without a subdomain tag. Untagged problems are not in the map; this is
    where they show up.
#}
with
    graded_problems as (
        select course_key as master_course, block_id as problem_block_id
        from {{ ref("dim_course_blocks") }}
        where
            block_type = 'problem'
            and graded
            and in_latest_publish
            and course_key
            in (select master_course from {{ ref("int_pss_master_course_scope") }})
    ),
    tagged as (
        select distinct problem_block_id
        from {{ ref("int_pss_quiz_item_subdomain_map") }}
    )
select
    graded_problems.master_course as master_course,
    count() as graded_problem_count,
    countIf(tagged.problem_block_id is not null) as tagged_graded_problem_count,
    countIf(tagged.problem_block_id is null) as untagged_graded_problem_count
from graded_problems
left join tagged on tagged.problem_block_id = graded_problems.problem_block_id
group by graded_problems.master_course settings join_use_nulls = 1
