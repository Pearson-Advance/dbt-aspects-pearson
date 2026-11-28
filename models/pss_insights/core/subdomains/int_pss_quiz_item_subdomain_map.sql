{{ config(tags=["pss_insights", "pss_insights_subdomains"]) }}

with
    -- An observed [] supersedes every earlier assignment. Keep the last
    -- observation of retired problems only for historical reconciliation.
    latest_problem_observation as (
        select *
        from {{ ref("int_pss_quiz_problem_objectives") }}
        where observed_at <= now64(6, 'UTC')
        qualify
            row_number() over (
                partition by master_course, problem_block_id order by observed_at desc
            )
            = 1
    ),
    assignments as (
        select *, arrayJoin(objectives) as objective from latest_problem_observation
    ),
    assignment_provenance as (
        select object_id, taxonomy, tag, max(id) as object_tag_id
        from {{ ref("int_tags_object_tag_current") }}
        group by object_id, taxonomy, tag
    ),
    problems as (
        select course_key, block_id, subsection_block_id, graded, in_latest_publish
        from {{ ref("dim_course_blocks") }}
        where block_type = 'problem'
    )
select
    a.master_course as master_course,
    a.problem_block_id as problem_block_id,
    if(in_latest_publish, a.subsection_block_id, null) as subsection_block_id,
    coalesce(b.graded, false) as graded,
    a.is_latest_capture and coalesce(b.in_latest_publish, false) as in_latest_publish,
    b.block_id is not null as in_course_structure,
    toInt32(a.objective .1) as taxonomy_id,
    t.name as taxonomy_name,
    nullIf(toInt32(a.objective .2), 0) as domain_tag_id,
    if(domain_tag_id is null, null, a.objective .4) as domain_name,
    if(domain_tag_id is null, null, a.objective .6) as domain_external_id,
    nullIf(toInt32(a.objective .3), 0) as subdomain_tag_id,
    if(subdomain_tag_id is null, null, a.objective .5) as subdomain_name,
    if(subdomain_tag_id is null, null, a.objective .7) as subdomain_external_id,
    a.objective .8 as lineage,
    length(a.objectives) as item_subdomain_count,
    p.object_tag_id as object_tag_id,
    a.observed_at as observed_at,
    a.capture_id as capture_id,
    a.mapping_status as mapping_status
from assignments as a
left join
    problems as b on b.course_key = a.master_course and b.block_id = a.problem_block_id
left join
    {{ ref("int_pss_quiz_item_structure_coverage") }} as q
    on q.master_course = a.master_course
    and q.subsection_block_id = b.subsection_block_id
left join {{ ref("dim_most_recent_taxonomies") }} as t on t.id = toInt32(a.objective .1)
left join
    assignment_provenance as p
    on p.object_id = a.problem_block_id
    and p.taxonomy = toInt32(a.objective .1)
    and p.tag = toInt32(if(a.objective .3 = 0, a.objective .2, a.objective .3))
where
    not coalesce(b.in_latest_publish, false)
    or (
        a.is_latest_capture
        and a.subsection_block_id = b.subsection_block_id
        and (q.structure_status is null or q.structure_status = 'counts_match')
    )
    settings join_use_nulls = 1,
    final = 1
