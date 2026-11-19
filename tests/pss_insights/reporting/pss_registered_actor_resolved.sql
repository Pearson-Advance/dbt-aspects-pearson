{{ config(enabled=var("pss_reporting_enabled", false), tags=["pss_reporting"]) }}

-- Unresolved registered members invalidate denominators even if the resolved
-- roster itself has unique keys. PADV-3833 must reconcile them before release.
select ccx_id
from {{ ref("int_pss_learner_class_context") }}
where is_registered and empty(ifNull(actor_id, ''))
