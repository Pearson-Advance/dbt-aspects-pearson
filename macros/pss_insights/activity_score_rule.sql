{% macro pss_activity_score_rule(master_course, activity_type) %}
    {% set rules = var("pss_current_score_rules", {}) %}
    {% if rules is not mapping %}
        {{
            exceptions.raise_compiler_error(
                "pss_current_score_rules must map exact master course keys to activity type and rule"
            )
        }}
    {% endif %}
    {% for master, activities in rules.items() %}
        {% if master is not string or not master.startswith(
            "course-v1:"
        ) or activities is not mapping %}
            {{
                exceptions.raise_compiler_error(
                    "Invalid master key or activity configuration in pss_current_score_rules"
                )
            }}
        {% endif %}
        {% for activity, rule in activities.items() %}
            {% if activity not in [
                "Quiz",
                "Lab",
                "Practice Exam",
                "Final Exam",
            ] or rule not in ["lms_subsection", "measureup_latest_finished"] %}
                {{
                    exceptions.raise_compiler_error(
                        "Unknown PSS activity type or current score rule"
                    )
                }}
            {% endif %}
        {% endfor %}
    {% endfor %}
    {% set encoded = tojson(rules) | replace("\\", "\\\\") | replace("'", "''") %}
    ifNull(
        nullIf(
            JSONExtractString(
                '{{ encoded }}', {{ master_course }}, {{ activity_type }}
            ),
            ''
        ),
        'agreement'
    )
{% endmacro %}
