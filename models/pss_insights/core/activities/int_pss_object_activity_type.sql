{{ config(tags=["pss_insights", "pss_insights_activities"]) }}

{#
    Precedence rule: tag deletions are not exported, so an object can hold
    several activity-type assignments. The most recent one wins (highest
    ObjectTag id, since re-adding a tag creates a new id), and
    activity_type_assignment_count > 1 flags the object.

    The value comes from the current tag row (by tag id), so a renamed tag is
    picked up; ObjectTag._value is the fallback when the tag row is missing
    (join_use_nulls = 0 fills it with '').
#}
with
    assignments as (
        select
            object_tag.id as object_tag_id,
            object_tag.object_id,
            object_tag.tag,
            object_tag._value
        from {{ ref("int_tags_object_tag_current") }} as object_tag
        where
            has(
                {{ pss_taxonomy_ids_array("pss_activity_type_taxonomy_id") }},
                object_tag.taxonomy
            )
    ),
    latest as (
        select
            object_id,
            max(object_tag_id) as activity_type_object_tag_id,
            argMax(tag, object_tag_id) as activity_type_tag_id,
            argMax(_value, object_tag_id) as object_tag_value,
            count() as activity_type_assignment_count
        from assignments
        group by object_id
    ),
    tags as (
        select id, value
        from {{ ref("dim_most_recent_tags") }}
        where id in (select activity_type_tag_id from latest)
    )
select
    latest.object_id as object_id,
    latest.activity_type_object_tag_id as activity_type_object_tag_id,
    latest.activity_type_tag_id as activity_type_tag_id,
    coalesce(nullIf(tags.value, ''), latest.object_tag_value) as activity_type_value,
    activity_type_value in {{ pss_activity_types_sql() }} as is_recognized_value,
    latest.activity_type_assignment_count as activity_type_assignment_count
from latest
left join tags on tags.id = latest.activity_type_tag_id settings final = 1
