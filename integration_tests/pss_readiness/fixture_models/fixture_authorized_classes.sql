-- Both grants cover the same class. The reports must not multiply either learner.
select institution_id, ccx_id
from {{ ref("int_pss_instructor_class_access") }}
where principal_username in ('coach71', 'coach72') and has_instructor_grant
