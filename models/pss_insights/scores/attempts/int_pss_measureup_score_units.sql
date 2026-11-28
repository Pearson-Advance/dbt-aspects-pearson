{{ config(tags=["pss_scores"]) }}

select
    master_course,
    any(score_unit) as approved_unit,
    any(accept_version_pairing) as version_pairing,
    count() = 1
    and approved_unit in ('', 'fraction', 'percent')
    and version_pairing in (0, 1) as valid_policy
from {{ ref("pss_measureup_score_units") }}
group by master_course
