# PSS class and master-course reporting

These reporting models supply the shared calculations for Instructor Class Insights and Site Admin Course Insights. The dashboard datasets bind the authenticated principal, dates and charts to these contracts.

| Current report | Stable row key | Measures |
| --- | --- | --- |
| `rpt_pss_class_metrics` | CCX | Class dates, roster and readiness counts, risk, attention, all support reasons, grade and content progress, practice/final averages, weakest practice objective, final pass rate and delivery totals. Includes eligible classes with empty rosters or unverified inventory. |
| `rpt_pss_learner_progress` | CCX and registered actor | LMS course grade, observed and verified unit visits, readiness fraction/band/reason, score sums and counts, qualifying Final Exam attempts and learner pass outcome. The same actor ID can have memberships in several classes. |
| `rpt_pss_class_activity_delivery` | CCX and applicable published master subsection | Type, order, registration, observed and verified participants, completion and difficulty. Includes published activities with no sitting. Sort Quiz/Lab difficulty by `activity_sequence`. |

Each row carries its institution and master course. Scores and proportions are fractions on 0–1 except activity score columns ending in `_pct`, which use 0–100. Use IDs for identity and names for display.

## Upstream evidence

| Producer | Inputs used |
| --- | --- |
| [Class context](../context/README.md) | Eligible CCX and institution, local effective dates, resolved memberships, independently verified roster denominator and principal-to-class access. Observed resolved registrations can be known before the complete population is verified. |
| [Activity inventory](../core/README.md) | Published subsection catalog, exact publication and class applicability, order, current grading policy, native Final Exam mark or MeasureUp cut-score rule. |
| [Scores and attempts](../scores/README.md) | Current learner/activity scores and distinct linked whole sittings, UTC starts, participation, eligible practice and qualifying final flags, provider pass evidence and verified version-specific cut scores. Bounded attempt coverage comes from the reconciled activity ledger. |
| [Readiness](../readiness/README.md) | `rpt_pss_learner_readiness_current` provides the per-membership fraction, band and unavailable reason. `rpt_pss_practice_subdomain_results` provides all eligible objective buckets. The class ranking uses the learner ranking's **status** and reconciled practice histories to assess gaps. It pools counts from the full detail, including objectives outside every learner's top three. |
| LMS and Aspects | Current course-grade-calculated xAPI evidence supplies the LMS grade fraction. A recorded zero contributes to the mean. Missing or contradictory scores remain unavailable. Navigation counts unique visits to applicable published units. Use the exact CCX tree when available and the reviewed class applicability for a master fallback. |

## Authorization and time

Build the authorized relation from `int_pss_instructor_class_access` using the authenticated identity mapped by the integration. Pass only its `institution_id` and `ccx_id` to the calculation. Apply the same pair filter to direct reads of the three reporting tables. Restrict raw reporting and operator schema grants accordingly. The class-coverage adapter and the unlinked-attempt diagnostic live in the restricted schema and can identify individual actors. Repeated instructor assignments do not multiply facts. After the access relation reflects a revocation, the next scoped query excludes that class without waiting for a class snapshot refresh. [Principal lookup contract](../context/README.md)

| Query entry point | What changes with the selection |
| --- | --- |
| `pss_master_course_summary(authorized_classes, reporting_date)` | Computes distinct active classes and learners from authorized membership-level records. Effective start and end days are inclusive. Missing bounds produce a diagnostic count. A historical reporting date evaluates the current known bounds and membership snapshots. |
| `pss_class_activity_delivery(start_utc, end_utc, authorized_classes)` | Filters qualified sittings by UTC start before joining them to current eligible learners and published activities. The lower bound is inclusive and upper bound exclusive. A window can change the participant count, completion and observed attempt average. The current learner/activity difficulty average remains a snapshot. |
| `pss_class_delivery_by_type(authorized_classes, start_utc, end_utc)` | Reports completed, total and remaining for each activity type, including types with zero published activities. Omitting both dates uses the materialized current result. |
| Learner readiness and weakness for a selected period | Use the readiness scoped period macros when the analysis requires a newly calculated score or ranking. Filtering a class snapshot by refresh time does not recompute historical readiness. |

The packaged [master summary](../../../integration_tests/pss_reporting/fixture_models/fixture_reporting_master.sql) and [period delivery](../../../integration_tests/pss_reporting/fixture_models/fixture_reporting_period.sql) show calls with an authorized relation.

## Candidate definitions for Product review

| Decision | Calculation exposed for review |
| --- | --- |
| Readiness denominator | Count On Target, Borderline, At Risk and unavailable. Divide by learners with a calculated band for a verified roster. Low starts at 80% On Target, Medium at 70%, and High is below 70%. A zero denominator leaves risk NULL. Medium and High need attention. For High risk, return every available Quiz, Lab or Practice Exam mean strictly below 70%. |
| Learners in multiple visible active classes | Count each registered actor once per master. A missing or conflicting membership band keeps that actor’s master readiness unavailable. Expose the two causes and any unverified roster separately. |
| Final Exam retakes and pass evidence | Divide distinct qualifying passers by distinct qualifying attempters. Any qualifying pass wins across visible memberships. Adjudicate a score using the rule for that exam. Disagreement with a recorded provider pass flag or unknown eligible evidence leaves the outcome unresolved if it could change the result. No attempters gives a NULL rate. The native-mark path has a unit fixture. Production native Final Exam outcomes require an authentic qualifying whole sitting from the producer. |
| Weighting | Practice and Final Exam means weight each eligible scored attempt equally. Quiz and Lab means weight each available current learner/activity score equally. An item-weighted practice subdomain uses pooled correct divided by pooled total. Show the counts and coverage status with the weakest subdomain. |
| Content progress | Use distinct visited units divided by the applicable published units, with source reconciliation. Product and the LMS owner must identify the appropriate completion feed if they select completed units. |
| Period scope and empty rosters | Use the current roster, publication and policy. Use UTC attempt windows and the configured reporting timezone for class dates (UTC by default). An empty roster retains its known activity total and has NULL completed/remaining. An incomplete inventory or unresolved roster cannot certify a class completion total. |

If A has one passer in one qualifying learner, and B has one passer in four, A's passer may also have failed in B. If the B passer is a different person, the combined population has two distinct passers in four distinct qualifying learners. The course rate is **50%**. The mean of the two class percentages would be **62.5%**. Restricting access to B returns **25%**.

## Reconciliation and refresh

The default project builds the reports with the producer graph. Publication, policy, eligibility, coverage and native source approvals retain the upstream defaults. The two additional seeds ship header-only. Operators can supply reviewed relations via `pss_class_coverage_model` and `pss_activity_coverage_model`, with the same fields as the respective seeds.

- `pss_reporting_class_coverage_evidence` binds all-history final participation and navigation coverage to an institution, CCX and current publication. Set `final_attempts_complete` or `navigation_complete` only after independently reconciling the applicable source population, class scope and history. The navigation check includes unit inventory. A persisted sitting with unresolved activity attribution can invalidate its owner’s negative final outcome while a known pass remains usable. Rows require `verified_at`, `valid_until` and an auditable `evidence_ref`.
- `pss_activity_history_coverage` certifies all-history participation for the same scope and a specific published subsection. A covered finite window can also use the score models' `pss_attempt_coverage_evidence`. Absent proof, retain observed counts and withhold uncertain totals. A verified nonempty roster whose every member already has a qualifying sitting proves completion directly.
- Review and renew certificates at a common source cutoff. Expired, conflicting, wrong-publication or absent evidence cannot certify the affected result. A materialized result reflects the evidence available at its last successful refresh. After evidence is withdrawn, refresh the affected dependency chain before exposing a revised snapshot.

All five reporting tables use four-hour replacement refreshes. The restricted table of unlinked MeasureUp sittings retains the source exam attempt ID and follows the same course-structure refresh as the canonical sitting table. Delivery, learner progress and class metrics all wait for this shared result. Class/catalog coverage follows course structure. Delivery follows the catalog, canonical attempts and current activity scores. Learner progress follows structure, the catalog, attempts, scores and current readiness. Class metrics follows learner progress, delivery, catalog, complete practice detail and learner practice ranking status. The ClickHouse `*_mv` definitions declare these dependencies. Check `system.view_refreshes` for last success, duration, rows, failures and the next scheduled refresh. The interval is a schedule. Source export lag and failed parents affect actual freshness.

## Validation and release

Use the pinned dbt environment and Docker with local source directories for the deployment plugin containing the published-assignment DDL and the Aspects 3.0.3 migrations. The replay creates a disposable ClickHouse server and uses producer-schema records. Its extra dbt models implement caller scope and date selections outside deployment paths.

```sh
dbt deps
make validate
python scripts/validate_course_licensing_models.py
make format-check
dbt parse --profiles-dir .github --no-partial-parse
python integration_tests/pss_reporting/run.py \
  --tutor-pearson /path/to/deployment-plugin \
  --tutor-aspects /path/to/analytics-plugin
```

- [ ] **Staging.** Reconcile source records and timestamps with the reporting grain. Verify access grants and revocation, unequal/shared populations, grade and visit telemetry, exam eligibility/version, catalog coverage and unopened activities. Record source watermarks, two scheduled refreshes, representative read volume, memory, latency, freshness and existing Course Licensing/MeasureUp regression counts. Obtain Product decisions on the candidate definitions above.
- [ ] **Production.** After staging acceptance and release approval, repeat the scoped population, KPI, eligibility, access and freshness checks for an approved class. Attach the deployed revisions and evidence.
- **Rollback.** Restore affected dataset references to their previous revision. Revert the reporting definitions and, where required, remove only its refreshable views and their owned target tables in reverse dependency order. Preserve upstream relations and producer data.
