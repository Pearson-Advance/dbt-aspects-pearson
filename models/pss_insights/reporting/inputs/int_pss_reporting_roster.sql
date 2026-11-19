-- PADV-3833 resolves the stable actor and the registration status. Instructor
-- assignments are deliberately absent from the grain of all three reports.
select distinct ccx_id, assumeNotNull(actor_id) as actor_id
from {{ ref("int_pss_learner_class_context") }} as membership
where is_registered and notEmpty(ifNull(membership.actor_id, ''))
