{{ config(tags=["pss_insights", "pss_insights_subdomains"]) }}

with
    taxonomy_scope as (
        select
            scope.master_course as master_course,
            uniqExact(scope.taxonomy_id) as taxonomy_count,
            uniqExactIf(scope.taxonomy_id, dictionary.id > 0) as available_count
        from {{ ref("int_pss_quiz_taxonomy_scope") }} as scope
        left join
            {{ ref("dim_most_recent_taxonomies") }} as dictionary
            on dictionary.id = scope.taxonomy_id
        group by scope.master_course
    ),
    quiz_sections as (
        select master_course, subsection_block_id, structure_status, 1 as is_quiz
        from {{ ref("int_pss_quiz_item_structure_coverage") }}
    ),
    quiz_checks as (
        select
            master_course,
            countIf(structure_status != 'counts_match') as mismatched_sections
        from quiz_sections
        group by master_course
    ),
    latest_observations as (
        select
            master_course,
            problem_block_id,
            subsection_block_id,
            mapping_status,
            length(objective_keys) as assignment_count,
            arrayCount(x -> x .3 != 0, objective_keys) as subdomain_count
        from {{ ref("int_pss_quiz_problem_objectives") }}
        where is_latest_capture
    ),
    counts as (
        with
            ifNull(o.mapping_status, '') != 'observed'
            or empty(ifNull(o.subsection_block_id, ''))
            or ifNull(o.subsection_block_id, '')
            != b.subsection_block_id as mapping_unknown
        select
            b.course_key as master_course,
            uniqExact(b.block_id) as graded_count,
            uniqExactIf(b.block_id, mapping_unknown) as graded_unknown,
            uniqExactIf(b.block_id, mapping_unknown and q.is_quiz = 1) as quiz_unknown,
            uniqExactIf(b.block_id, o.assignment_count > 0) as tagged_count,
            uniqExactIf(b.block_id, q.is_quiz = 1) as quiz_count,
            uniqExactIf(
                b.block_id, q.is_quiz = 1 and o.subdomain_count > 0
            ) as quiz_with_subdomain,
            uniqExactIf(
                b.block_id,
                q.is_quiz = 1 and o.assignment_count > 0 and o.subdomain_count = 0
            ) as quiz_domain_only
        from {{ ref("dim_course_blocks") }} as b
        left join
            latest_observations as o
            on b.course_key = o.master_course
            and b.block_id = o.problem_block_id
        left join
            quiz_sections as q
            on b.course_key = q.master_course
            and b.subsection_block_id = q.subsection_block_id
        where
            b.block_type = 'problem'
            and b.graded
            and b.in_latest_publish
            and b.course_key
            in (select master_course from {{ ref("int_pss_master_course_scope") }})
        group by b.course_key
    )
select
    s.master_course as master_course,
    ifNull(t.taxonomy_count, 0) as configured_taxonomy_count,
    ifNull(t.available_count, 0) as available_taxonomy_count,
    configured_taxonomy_count > 0
    and available_taxonomy_count
    = configured_taxonomy_count as taxonomy_scope_available,
    multiIf(
        ifNull(k.mismatched_sections, 0) > 0
        or i.inventory_status = 'structure_disagrees',
        'structure_disagrees',
        i.inventory_status in ('classified', 'no_graded_subsections'),
        'counts_match',
        i.inventory_status in ('graded_not_classified', 'partially_classified'),
        'classification_incomplete',
        'publication_unavailable'
    ) as quiz_structure_status,
    ifNull(c.graded_count, 0) as graded_problem_count,
    if(
        taxonomy_scope_available
        and quiz_structure_status
        not in ('structure_disagrees', 'publication_unavailable')
        and ifNull(c.graded_unknown, 0) = 0,
        ifNull(c.tagged_count, 0),
        null
    ) as tagged_graded_problem_count,
    toUInt64(
        graded_problem_count - tagged_graded_problem_count
    ) as untagged_graded_problem_count,
    if(
        quiz_structure_status = 'counts_match', ifNull(c.quiz_count, 0), null
    ) as quiz_problem_count,
    if(
        taxonomy_scope_available
        and quiz_structure_status = 'counts_match'
        and ifNull(c.quiz_unknown, 0) = 0,
        ifNull(c.quiz_with_subdomain, 0),
        null
    ) as quiz_problems_with_subdomain,
    if(
        taxonomy_scope_available
        and quiz_structure_status = 'counts_match'
        and ifNull(c.quiz_unknown, 0) = 0,
        ifNull(c.quiz_domain_only, 0),
        null
    ) as quiz_problems_domain_only,
    toUInt64(
        quiz_problem_count - quiz_problems_with_subdomain - quiz_problems_domain_only
    ) as quiz_problems_without_mapping
from {{ ref("int_pss_master_course_scope") }} as s
left join counts as c on c.master_course = s.master_course
left join taxonomy_scope as t on t.master_course = s.master_course
left join quiz_checks as k on k.master_course = s.master_course
left join
    {{ ref("int_pss_master_inventory_coverage") }} as i
    on i.master_course = s.master_course
