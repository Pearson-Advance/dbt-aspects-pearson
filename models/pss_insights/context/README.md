# PSS class, learner and access context

This context supplies the current population for Class Insights and Course Insights. Dates, membership and grants have different grains. Filtering by date evaluates the current effective class interval. It does not reconstruct prior enrollments, permissions or class configurations.

| dbt relation | Grain | Consumer |
| --- | --- | --- |
| `int_pss_class_context` | One nonempty CCX key among current application-undeleted classes | Class population and master for the activity inventory; dates and KPIs for class reporting |
| `int_pss_learner_class_context` | One resolved xAPI actor and CCX | Score and readiness evidence, and class reporting progress. Join on both keys. A person in two classes has two memberships. |
| `int_pss_roster_coverage` | One row per class, including classes with no observed enrollments | Class reporting roster denominators, pending and unresolved counts |
| `int_pss_instructor_class_access` | One authenticated LMS principal and permitted institution/CCX pair | Class reporting and dashboard query-time authorization, including institution administrators |
| `int_pss_roster_evidence` | One class and source person, invitation or unresolved enrollment key | Restricted reconciliation. Contains LMS IDs and source keys. |

All are ordinary views. Course Licensing's insert-triggered materialized views already aggregate the latest event per source ID. Their current views suppress sync tombstones. CCX overviews, learner identifiers and login names come from the pinned Aspects event sinks. Access changes are visible on a new query after the current source changes. Measure transport, replay, replica and application-cache lag in the target environment. A missed LMS bulk update or direct deletion can persist until source reconciliation.

## Population and dates

Until Product and Licensing decide class eligibility for disabled, deleted or missing licenses, the proposal includes non-deleted classes across license states. Inspect `class_scope_status`, `license_record_status`, `class_license_status` and `current_license_status` to quantify exclusions or discrepancies. Multiple live source rows for a CCX retain one diagnostic row with null ownership keys. Master resolution checks the assigned master against the CCX locator. A missing assignment may be derived from a valid locator. Malformed or conflicting keys have a null master.

The LMS writes each CCX's effective start and end to its own CourseOverview. The view reads that exact key. Enrollment expiry, scheduled events and master-only exports cannot establish a CCX interval. Set `pss_reporting_timezone` to the agreed business timezone (default UTC). Use the inclusive `pss_context_active_on(class_start, class_end, reporting_date)` macro for a selected date. Missing, unorderable and inverted evidence leave activity status unavailable.

## Roster completeness

Active licensed enrollment provides the provisional affirmative registration evidence. Inactive, Expired and conflicting current states retain their raw statuses and a nullable registration flag pending the decision on how enrollment states map to registered learners. Unclaimed invitations have their own count. Only a globally one-to-one, typed xAPI external UUID resolves a learner. The mapping works for learners with no activity. The standard Open edX external-ID xAPI configuration emits the UUID as `actor.account.name`. Reconcile this setting and representative actors against LMS user IDs in the target, including numeric usernames if present. A missing identity feed leaves affected learners unresolved.

Observed counts remain queryable for an unverified extract. `registered_actor_denominator` becomes available only for a fully resolved population in a reconciled target. Following a full target reconciliation and approval of the refresh interval, set `pss_roster_reconciled_until_utc` to its expiry in `YYYY-MM-DD HH:MM:SS` UTC and rebuild the coverage view. Empty, invalid or expired values yield NULL. Validity is evaluated at query time. A sample reconciliation cannot attest the entire target. The current sources provide no automatic completeness watermark.

## Authorization

The grant lookup and roster evidence reside in the restricted schema (`reporting_pss_restricted` with the standard target). A direct instructor assignment requires an active instructor and matching active institution affiliation. An active institution administrator receives only that institution's classes. An unknown or ambiguous login yields no grants. Platform and the reporting owner must approve the login mapping and eligible roles for each dashboard deployment. The admin feed records assignment activity. The authentication service supplies current account status.

Select the lookup column from the authenticated identity contract. For an embedded token whose issuer supplies the LMS user ID, match the verified ID to `principal_user_id`. For an identity provider that supplies the LMS username, match its exact case to `principal_username`. A numeric login can belong to another LMS user. Use only the identifier guaranteed by the issuer. Reject blank and generic guest identities and client-supplied identity filters.

Use the grant lookup as a semi-join before class or master aggregation. This example counts distinct actors with registered membership evidence when the portal issuer supplies an authenticated LMS user ID.

```sql
SELECT count(DISTINCT m.actor_id)
FROM reporting.int_pss_learner_class_context AS m
INNER JOIN reporting.int_pss_class_context AS c ON m.ccx_id = c.ccx_id
WHERE m.is_registered = 1
  AND (c.institution_id, c.ccx_id) IN (
      SELECT institution_id, ccx_id
      FROM reporting_pss_restricted.int_pss_instructor_class_access
      WHERE principal_user_id = {authenticated_lms_user_id:String}
  )
```

For whole-population KPIs also check the coverage of every included class. The restricted grant view uses the dbt deployment identity as SQL SECURITY DEFINER. An explicitly authorized backend account can read that projection without direct access to profiles or roster diagnostics. Limit it to the query service, and limit evidence access to reconciliation operators. Verify target-schema privileges, the view owner, tenant filtering and result-cache behavior before serving dashboards. The dashboard datasets enforce access.

## Reconciliation and release

1. Record the environment, app/dbt revisions, reporting timezone and UTC cutoff. Capture the independent LMS class, license, enrollment, identity, invitation and assignment extracts and ClickHouse state at the same cutoff. Use a consistent snapshot or a settled interval and record any in-flight changes. Keep identifiable records in the restricted environment.
2. Load the LMS expectation into the restricted schema as `pss_lms_context_at_cutoff(record_type String, ccx_id String, subject_key String, attributes Array(Nullable(String)))`. Supply one row per business key under the candidate inclusion rules using the layout below. Use SQL NULL for unknowns and strings `1`/`0` for flags.
3. Run `dbt compile --select pss_context_reconciliation`. Execute the compiled [analysis](../../../analyses/pss_context_reconciliation.sql) with the reconciliation role. It reports extra and missing keys, differing attributes and differing row counts. A full comparison yields no rows when the two snapshots agree. For a sample extract, filter the final result to the independently selected CCX keys. Verify coverage across the full population separately.
4. Compare class counts grouped by `is_deleted`, `deleted` and `license_status` in `int_course_licensing_institution_ccx_latest_state`, then by `license_record_status`, `master_status`, `institution_status` and `class_date_status` in the class context. Report counts and percentages against the source population, including blank CCX IDs. Reconcile enrollments via the restricted source keys and identity reasons. Inspect source records that match neither the class key nor its InstitutionCCX ID separately. A conflicting enrollment can affect two class coverage records.
5. In staging, check a real override, shared learner, untouched learner, pending invitation, missing identity and an assignment revocation. Record the effective LMS change time, sink arrival, first denied direct query and first denied dashboard query. Confirm the other instructor retains access. Reconcile existing Course Licensing and MeasureUp counts at the same cutoff. Record source lag, maximum accepted revocation delay and query timings at representative volume.
6. Production checks follow staging acceptance and release approval. For rollback, disconnect dependent datasets, revoke permissions to the new projections and revert the dbt revision. Remove these new views in reverse dependency order as part of the approved rollback. Reverting Git alone leaves built views and database privileges in place.

| record_type | subject_key | attributes in order |
| --- | --- | --- |
| class | empty string | InstitutionCCX ID, institution ID, master course key, license ID, effective license status, start and end as ISO dates |
| membership | xAPI UUID | Observed enrollment status, nullable registration flag |
| roster | empty string | Registered source learners, resolved registered learners, unresolved registered learners, pending invitation records |
| grant | LMS user ID | Institution ID, exact login, instructor flag, institution-admin flag |

## Local verification

With Python dependencies and dbt packages installed, run `make validate`, `python scripts/validate_course_licensing_models.py`, `make format-check` and `dbt parse --no-partial-parse`. Run `dbt run --select +tag:pss_context` and `dbt build --select tag:pss_context` on an isolated target with the Course Licensing sink DDL and the Aspects 3.0.3 event-sink migrations. On a fresh disposable instance using the standard database names, execute [the event replay](../../../integration_tests/pss_context/replay.sql) with `clickhouse-client --multiquery`. It checks duplicate events, exact CCX dates, shared membership, multiple grants, live revocation and an older event arriving after a tombstone.
