-- Test principal supplied by the harness. Production resolves the authenticated user.
select institution_id, ccx_id
from {{ ref("int_pss_instructor_class_access") }}
where principal_username = 'coach72'
