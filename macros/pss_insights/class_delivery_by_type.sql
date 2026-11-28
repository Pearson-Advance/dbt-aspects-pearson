{% macro pss_completed_activities(roster_size, total, listed, evaluated, completed) %}
    if(
        {{ roster_size }} > 0
        and {{ total }} = coalesce({{ listed }}, 0)
        and {{ total }} = coalesce({{ evaluated }}, 0),
        coalesce({{ completed }}, 0),
        null
    )
{% endmacro %}

{% macro pss_class_delivery_by_type(
    authorized_classes, period_start=none, period_end=none
) %}
    {% set classes %}
        (select *, registered_actor_denominator as registered_learners
        from {{ ref("int_pss_class_catalog_coverage") }})
    {% endset %}
    {% if period_start is none and period_end is none %}
        {% set delivery = ref("rpt_pss_class_activity_delivery") %}
    {% else %}
        {% set delivery = (
            "("
            ~ pss_class_activity_delivery(
                period_start, period_end, authorized_classes
            )
            ~ ")"
        ) %}
    {% endif %}
    {{ pss_delivery_type_counts(authorized_classes, classes, delivery) }}
{% endmacro %}

{% macro pss_delivery_type_counts(authorized_classes, classes, delivery) %}
    with
        scoped_classes as (
            select
                institution_id,
                ccx_id,
                master_course,
                catalog_status,
                known_published_activities,
                registered_learners
            from {{ classes }}
            where
                notEmpty(ifNull(institution_id, ''))
                and (institution_id, ccx_id)
                in (select institution_id, ccx_id from {{ authorized_classes }})
        ),
        activity_types as (
            select
                arrayJoin(
                    ['Quiz', 'Lab', 'Practice Exam', 'Final Exam']
                ) as activity_type
        ),
        counts as (
            select
                d.institution_id as institution_id,
                d.ccx_id as ccx_id,
                d.master_course as master_course,
                d.activity_type as activity_type,
                count() as listed,
                count(d.delivery_complete) as evaluated,
                countIf(d.delivery_complete = 1) as completed
            from {{ delivery }} as d
            where
                (d.institution_id, d.ccx_id)
                in (select institution_id, ccx_id from scoped_classes)
            group by d.institution_id, d.ccx_id, d.master_course, d.activity_type
        ),
        inventory as (
            select
                c.institution_id as institution_id,
                c.ccx_id as ccx_id,
                c.master_course as master_course,
                t.activity_type as activity_type,
                c.catalog_status as catalog_status,
                c.registered_learners as registered_learners,
                coalesce(d.listed, 0) as known_activities,
                if(
                    c.catalog_status = 'complete'
                    and c.known_published_activities = sum(coalesce(d.listed, 0)) over (
                        partition by c.institution_id, c.ccx_id, c.master_course
                    ),
                    known_activities,
                    null
                ) as total_activities,
                {{
                    pss_completed_activities(
                        "c.registered_learners",
                        "total_activities",
                        "d.listed",
                        "d.evaluated",
                        "d.completed",
                    )
                }} as completed_activities
            from scoped_classes as c
            cross join activity_types as t
            left join
                counts as d
                on c.institution_id = d.institution_id
                and c.ccx_id = d.ccx_id
                and isNotDistinctFrom(c.master_course, d.master_course)
                and t.activity_type = d.activity_type
        )
    select *, total_activities - completed_activities as remaining_activities
    from inventory
{% endmacro %}
