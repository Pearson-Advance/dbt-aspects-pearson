{#
    Open edX key helpers. Keys follow the opaque-keys formats:
      course    course-v1:ORG+COURSE+RUN
      block     block-v1:ORG+COURSE+RUN+type@TYPE+block@ID
      CCX       ccx-v1:ORG+COURSE+RUN+ccx@N
      CCX block ccx-block-v1:ORG+COURSE+RUN+ccx@N+type@TYPE+block@ID
#}
{# ORG+COURSE+RUN part of a block-v1 key. #}
{% macro pss_block_key_course_part(block_key) %}
    substring({{ block_key }}, 10, position({{ block_key }}, '+type@') - 10)
{% endmacro %}

{# course-v1 key of the course a block-v1 key belongs to. #}
{% macro pss_course_key_from_block_key(block_key) %}
    concat('course-v1:', {{ pss_block_key_course_part(block_key) }})
{% endmacro %}

{# course-v1 key a ccx-v1 key was created from; null if the key is malformed. #}
{% macro pss_course_key_from_ccx_key(ccx_key) %}
    if(
        startsWith({{ ccx_key }}, 'ccx-v1:') and position({{ ccx_key }}, '+ccx@') > 0,
        concat(
            'course-v1:',
            substring({{ ccx_key }}, 8, position({{ ccx_key }}, '+ccx@') - 8)
        ),
        null
    )
{% endmacro %}

{# CCX form of a block-v1 key, as MeasureUp and xAPI events report it. #}
{% macro pss_ccx_block_key(block_key, ccx_number) %}
    concat(
        'ccx-block-v1:',
        {{ pss_block_key_course_part(block_key) }},
        '+ccx@',
        {{ ccx_number }},
        substring({{ block_key }}, position({{ block_key }}, '+type@'))
    )
{% endmacro %}
