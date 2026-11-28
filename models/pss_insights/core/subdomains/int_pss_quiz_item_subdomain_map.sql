{{ config(tags=["pss_insights", "pss_insights_subdomains"]) }}

{#
    Only tags on course problem blocks count: a library item and its course
    copy have different keys. Resolve taxonomy IDs against the approved scope for the specific master run.

    Assumes two-level taxonomies: a top-level tag (parent 0 or null) is a
    domain; a child tag is a subdomain of its parent. If a parent tag is
    missing from dim_most_recent_tags, domain columns are null (warn test).

    Repeated assignments of the same tag (deletions are not exported) collapse
    to the most recent one. A domain-only tag is dropped when the item also
    has one of that domain's subdomains in the same taxonomy.

    item_subdomain_count counts the item's rows across every configured
    taxonomy; how credit is split is left to consumers.
#}
with
    assignments as (
        select
            object_tag.id as object_tag_id,
            object_tag.object_id as problem_block_id,
            {{ pss_course_key_from_block_key("object_tag.object_id") }}
            as master_course,
            object_tag.taxonomy as taxonomy_id,
            object_tag.tag as tag_id
        from {{ ref("int_tags_object_tag_current") }} as object_tag
        inner join
            {{ ref("int_pss_quiz_taxonomy_scope") }} as scope
            on scope.master_course
            = {{ pss_course_key_from_block_key("object_tag.object_id") }}
            and scope.taxonomy_id = object_tag.taxonomy
        where
            startsWith(object_tag.object_id, 'block-v1:')
            and position(object_tag.object_id, '+type@problem+') > 0
    ),
    tags as (
        select id, taxonomy, parent, value, external_id, lineage
        from {{ ref("dim_most_recent_tags") }}
        where
            taxonomy
            in (select taxonomy_id from {{ ref("int_pss_quiz_taxonomy_scope") }})
    ),
    resolved as (
        select
            assignments.master_course as master_course,
            assignments.problem_block_id as problem_block_id,
            assignments.taxonomy_id as taxonomy_id,
            assignments.object_tag_id as object_tag_id,
            tag.lineage as lineage,
            coalesce(tag.parent, 0) = 0 as is_domain_tag,
            if(is_domain_tag, tag.id, parent_tag.id) as domain_tag_id,
            if(is_domain_tag, tag.value, parent_tag.value) as domain_name,
            if(
                is_domain_tag, tag.external_id, parent_tag.external_id
            ) as domain_external_id,
            if(is_domain_tag, null, tag.id) as subdomain_tag_id,
            if(is_domain_tag, null, tag.value) as subdomain_name,
            if(is_domain_tag, null, tag.external_id) as subdomain_external_id
        from assignments
        inner join
            tags as tag
            on tag.id = assignments.tag_id
            and tag.taxonomy = assignments.taxonomy_id
        left join
            tags as parent_tag
            on parent_tag.id = tag.parent
            and parent_tag.taxonomy = tag.taxonomy
            and coalesce(parent_tag.parent, 0) = 0
    ),
    deduplicated as (
        select
            master_course,
            problem_block_id,
            taxonomy_id,
            domain_tag_id,
            subdomain_tag_id,
            max(object_tag_id) as latest_object_tag_id,
            argMax(domain_name, object_tag_id) as domain_name,
            argMax(domain_external_id, object_tag_id) as domain_external_id,
            argMax(subdomain_name, object_tag_id) as subdomain_name,
            argMax(subdomain_external_id, object_tag_id) as subdomain_external_id,
            argMax(lineage, object_tag_id) as lineage
        from resolved
        group by
            master_course,
            problem_block_id,
            taxonomy_id,
            domain_tag_id,
            subdomain_tag_id
    ),
    kept as (
        select *
        from deduplicated
        qualify
            subdomain_tag_id is not null
            or countIf(subdomain_tag_id is not null) over (
                partition by master_course, problem_block_id, taxonomy_id, domain_tag_id
            )
            = 0
    ),
    problems as (
        select course_key, block_id, subsection_block_id, graded, in_latest_publish
        from {{ ref("dim_course_blocks") }}
        where
            block_type = 'problem'
            and course_key
            in (select master_course from {{ ref("int_pss_master_course_scope") }})
    ),
    taxonomies as (select id, name from {{ ref("dim_most_recent_taxonomies") }})
select
    kept.master_course as master_course,
    kept.problem_block_id as problem_block_id,
    if(
        problems.in_latest_publish, nullIf(problems.subsection_block_id, ''), null
    ) as subsection_block_id,
    coalesce(problems.graded, false) as graded,
    coalesce(problems.in_latest_publish, false) as in_latest_publish,
    problems.block_id is not null as in_course_structure,
    kept.taxonomy_id as taxonomy_id,
    taxonomies.name as taxonomy_name,
    kept.domain_tag_id as domain_tag_id,
    kept.domain_name as domain_name,
    kept.domain_external_id as domain_external_id,
    kept.subdomain_tag_id as subdomain_tag_id,
    kept.subdomain_name as subdomain_name,
    kept.subdomain_external_id as subdomain_external_id,
    kept.lineage as lineage,
    count() over (
        partition by kept.master_course, kept.problem_block_id
    ) as item_subdomain_count,
    kept.latest_object_tag_id as object_tag_id
from kept
left join
    problems
    on problems.course_key = kept.master_course
    and problems.block_id = kept.problem_block_id
left join
    {{ ref("int_pss_quiz_item_structure_coverage") }} as quiz
    on problems.course_key = quiz.master_course
    and problems.subsection_block_id = quiz.subsection_block_id
left join taxonomies on taxonomies.id = kept.taxonomy_id
where
    ifNull(problems.in_latest_publish, false) = false
    or empty(ifNull(quiz.structure_status, ''))
    or quiz.structure_status = 'counts_match'
    settings join_use_nulls = 1,
    final = 1
