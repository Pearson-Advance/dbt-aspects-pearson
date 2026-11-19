{{ config(tags=["pss_insights"], severity="warn") }}

{#
    In-scope master courses with no catalog rows, by reason:
      no_graded_subsections   no graded subsection in the latest publish (no
                              graded content, or structure not exported)
      graded_not_classified   graded subsections exist but none has a
                              recognized activity type (pending tagging)
#}
with
    catalog_courses as (
        select distinct master_course from {{ ref("int_pss_activity_catalog") }}
    ),
    graded as (
        select master_course, count() as graded_subsection_count
        from {{ ref("int_pss_activity_type_coverage") }}
        where graded and in_latest_publish
        group by master_course
    )
select
    scope.master_course as master_course,
    if(
        graded.graded_subsection_count > 0,
        'graded_not_classified',
        'no_graded_subsections'
    ) as reason,
    coalesce(graded.graded_subsection_count, 0) as graded_subsection_count
from {{ ref("int_pss_master_course_scope") }} as scope
left join graded on graded.master_course = scope.master_course
where
    scope.master_course not in (select master_course from catalog_courses)
    settings join_use_nulls = 1
