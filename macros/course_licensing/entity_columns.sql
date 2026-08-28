{#
    Canonical column contract for each Course Licensing entity's raw sink
    row (excluding source_id, which every mv_/latest_state model selects
    separately). Each entity's list is defined exactly once here and shared
    by both its mv_ latest-state model (course_licensing_argmax_state) and
    its finalized latest-state view (course_licensing_argmax_merge), so the
    two can never drift out of sync with each other. Column order matters:
    argMaxMerge unpacks the packed tuple positionally.
#}
{% macro course_licensing_institution_columns() %}
    {{
        return(
            [
                "id",
                "created",
                "modified",
                "name",
                "short_name",
                "active",
                "uuid",
                "external_id",
                "has_bulk_register",
                "support_link",
                "operation",
                "is_deleted",
                "source_updated_at",
                "time_last_dumped",
                "dump_id",
                "sink_event_id",
                "schema_version",
            ]
        )
    }}
{% endmacro %}

{% macro course_licensing_institution_administrator_columns() %}
    {{
        return(
            [
                "id",
                "created",
                "modified",
                "institution_id",
                "institution_name",
                "user_id",
                "user_name",
                "user_email",
                "active",
                "operation",
                "is_deleted",
                "source_updated_at",
                "time_last_dumped",
                "dump_id",
                "sink_event_id",
                "schema_version",
            ]
        )
    }}
{% endmacro %}

{% macro course_licensing_license_columns() %}
    {{
        return(
            [
                "id",
                "created",
                "modified",
                "license_name",
                "institution_id",
                "institution_name",
                "master_courses",
                "catalogs",
                "course_id",
                "course_access_duration",
                "status",
                "license_type",
                "operation",
                "is_deleted",
                "source_updated_at",
                "time_last_dumped",
                "dump_id",
                "sink_event_id",
                "schema_version",
            ]
        )
    }}
{% endmacro %}

{% macro course_licensing_license_order_columns() %}
    {{
        return(
            [
                "id",
                "created",
                "modified",
                "license_id",
                "license_name",
                "license_status",
                "order_reference",
                "purchased_seats",
                "active",
                "operation",
                "is_deleted",
                "source_updated_at",
                "time_last_dumped",
                "dump_id",
                "sink_event_id",
                "schema_version",
            ]
        )
    }}
{% endmacro %}

{% macro course_licensing_institution_ccx_columns() %}
    {{
        return(
            [
                "id",
                "institution_id",
                "institution_name",
                "license_id",
                "license_name",
                "license_status",
                "ccx_id",
                "ccx_name",
                "master_course",
                "master_course_name",
                "custom_course",
                "min_students_allowed",
                "deleted",
                "operation",
                "is_deleted",
                "source_updated_at",
                "time_last_dumped",
                "dump_id",
                "sink_event_id",
                "schema_version",
            ]
        )
    }}
{% endmacro %}

{% macro course_licensing_licensed_enrollment_columns() %}
    {{
        return(
            [
                "id",
                "institution_id",
                "institution_name",
                "institution_ccx_id",
                "license_id",
                "license_name",
                "license_status",
                "user_id",
                "student",
                "student_email",
                "class_name",
                "class_id",
                "enrollment_mode",
                "enrollment_created",
                "is_active",
                "end_date",
                "expired",
                "operation",
                "is_deleted",
                "source_updated_at",
                "time_last_dumped",
                "dump_id",
                "sink_event_id",
                "schema_version",
            ]
        )
    }}
{% endmacro %}

{% macro course_licensing_course_enrollment_allowed_columns() %}
    {{
        return(
            [
                "id",
                "email",
                "user_id",
                "course_id",
                "auto_enroll",
                "created",
                "operation",
                "is_deleted",
                "source_updated_at",
                "time_last_dumped",
                "dump_id",
                "sink_event_id",
                "schema_version",
            ]
        )
    }}
{% endmacro %}
