# PSS class and master-course reporting

PADV-3837 supplies the shared calculations for PADV-3838. The three reports are current snapshots of eligible registered learners, applicable published activities, current policy and all observed eligible attempts. The class context supplies one institution and master-course assignment per CCX. An actor ID identifies the same learner across classes.

| Report | Stable row key | Measures |
| --- | --- | --- |
| `rpt_pss_learner_progress` | CCX, actor | LMS course grade, observed and verified unit visits, readiness band and reason, score sums and counts, qualifying Final Exam attempts and adjudicated learner outcome |
| `rpt_pss_class_activity_delivery` | CCX, master subsection block | Ordered activity type and name, roster size, observed and verified participants, completion and score averages. Includes published activities with zero attempts. |
| `rpt_pss_class_metrics` | CCX | Roster and readiness counts, risk, attention, all instructor support reasons, LMS grade, content progress, Practice and Final Exam averages, weakest practice subdomain, pass rate and delivery totals. Includes classes with empty rosters or unknown inventory. |

Scores and proportions in the class and learner reports use 0–1. The two activity score fields ending in `_pct` use 0–100. Display the weakest practice subdomain using its name and use its domain and subdomain IDs for grouping. Its score pools correct and total eligible items across the registered learners before ranking. Quiz and Lab difficulty uses current learner/activity scores. Sort by `activity_sequence` within the course.

## Dependencies and enablement

The reporting folder is disabled on its own until the following dbt inputs are available. All names below the PADV-3833, 3835 and 3836 headings are **proposed integration contracts**. Reconcile their field names, nullability, units, eligibility and identity with the delivering changes before enabling. PADV-3834 is open in [PR #21](https://github.com/Pearson-Advance/dbt-aspects-pearson/pull/21). Its policy seed initially has no data.

| Owner | Inputs and evidence consumed |
| --- | --- |
| PADV-3833 | `int_pss_class_context` with unique nonempty `ccx_id`, `institution_id`, `institution_ccx_id`, `master_course`, `class_name`, `license_id`, `license_status` and effective nullable Date values `class_start` and `class_end`. `int_pss_learner_class_context` supplies `ccx_id`, globally resolved `actor_id` and approved `is_registered`. Reconcile unresolved identities and the class population before using roster denominators. `int_pss_instructor_class_access` supplies the principal-to-authorized-class relation for the dataset caller. |
| PADV-3834 | `int_pss_activity_catalog`, `int_pss_activity_type_coverage`, `int_pss_grading_policy` and `pss_ccx_block_key` from #21. Reconcile the catalog's class scope, effective CCX availability and latest master publication with PADV-3833. Only the current publication feeds delivery. |
| PADV-3835 | `rpt_pss_activity_scores_current` has one current score per CCX, actor and master subsection. `rpt_pss_activity_attempts` has one row per distinct whole-activity attempt, including retakes. Both carry `ccx_id`, `actor_id`, `subsection_block_id`, `activity_type` and nullable `score_pct` (0–100). Attempts also carry UTC `attempted_at`, normalized `provider` (`native` or `measureup` for finals), `is_qualifying_final`, `is_eligible_practice`, nullable Boolean `is_passed` and exact test-version `cut_score` (0–100). The provider's attempt ordinal and individual problem events do not establish eligibility for these reports. |
| PADV-3836 | `rpt_pss_learner_readiness_current` supplies `ccx_id`, `actor_id`, nullable `readiness_status` and `readiness_unavailable_reason`. `rpt_pss_practice_subdomain_results` supplies eligible practice-mode CCX/actor/attempt/subdomain evidence with stable `domain_id`, `subdomain_id`, `subdomain_name`, `correct_items` and `total_items`. |
| Pinned Aspects | `dim_learner_most_recent_course_grade` supplies the LMS scaled course grade. `fact_learner_page_visits` supplies visited block IDs. `dim_course_blocks` supplies published units (`block_type = 'unit'`) and subsections. |

Coverage is evidence about the feed, independent of whether a learner opened anything. Set the following dbt variables to names of reconciled upstream dbt models when available. Omitted variables supply no attestations. A missing attestation preserves observed evidence and makes an unproven zero or failure unavailable.

- `pss_class_coverage_model` supplies one row per `ccx_id` with Boolean `final_attempts_complete` and `navigation_complete` for the current class, applicable content and reporting history.
- `pss_activity_coverage_model` supplies `ccx_id`, `subsection_block_id`, `coverage_start` and `coverage_end` as nullable UTC timestamps. A row certifies continuous coverage on [start, end). A NULL bound explicitly certifies unbounded coverage in that direction. Supply merged continuous intervals. For all-time snapshots an interval must cover both ends. Where every registered learner is observed, completion is proven directly.

## Scoped course and period queries

A class or learner snapshot contains rows across the reporting population. Restrict dataset access to authorized classes before exposing rows or aggregating them. The master-course macro requires a relation containing the caller's authorized `(institution_id, ccx_id)` pairs. Multiple instructors or roles can grant the same class. The macro uses a semi-join, so additional grants do not multiply facts. Require PADV-3833 to verify grants and revocations and PADV-3838 to apply the same scoped relation to every dataset.

PADV-3838 can compile this expression into a dataset query with a CTE named `permitted_classes`. That CTE must resolve the authenticated principal’s grants at query time, including revocations. An empty or unknown institution does not grant access. The following call returns one row per master with at least one permitted active class. The date selects classes whose effective start and end include that day. Other evidence still describes the current snapshot.

```jinja
{{ pss_master_course_summary("permitted_classes", "toDate(now('UTC'))") }}
```

Final pass rate counts a learner once across all visible active classes for the master. With one learner passing in class A and that same learner failing alongside one pass and two failures in class B, A is 1/1 and B is 1/4. The shared learner and one class B learner passed among four distinct people, so the master result is **2/4 = 50%**. Averaging the class rates gives 62.5%. Dropping a class from the authorized scope reruns the deduplication on the remaining records.

The activity-date filter has a different meaning. `pss_class_activity_delivery(start, end)` restricts attempt evidence to [start, end) UTC and then joins it to the entire current applicable inventory. Pass SQL timestamp expressions for the bounds. The per-type summary is available through `pss_class_delivery_by_type(authorized_classes, period_start, period_end)`. With omitted bounds it reads the stored delivery snapshot. Current scores, enrollment, policy and publication are snapshots even when participation is recalculated for a period. An historical roster, historical readiness or historical LMS grade requires dated upstream inputs. Filtering a stored snapshot's rows by an attempt date would lose activities with no attempts.

## Definitions awaiting owner review

| Affected output | Provisional calculation to approve |
| --- | --- |
| Class risk and readiness proportions | Use only learners with a calculated band in the denominator. Show registered and unavailable counts. Should registered learners without readiness instead enter the denominator? No available learners leaves all proportions and risk NULL. |
| Shared learners | Count each actor once inside the selected master and authorized active classes. Use a readiness band only when all their selected memberships agree and have a band. Should one class take precedence when bands differ or one is unavailable? |
| Final pass rate | Any qualifying pass counts. A qualifying attempt below the applicable mark fails. Use the native final-specific mark or MeasureUp version cut score. If the provider pass flag disagrees with the computed result, keep that attempt unresolved. A missing provider flag with valid score and cut score uses the calculated result. Should the last attempt override an earlier pass, or the provider flag override the threshold? Confirm which finished attempts/modes PADV-3835 qualifies. No qualifying attempts yields NULL. |
| Score weighting | Practice and Final Exam KPIs pool available eligible attempt scores, including retakes. Quiz and Lab pool available current learner/activity scores. Approve attempt weighting versus equal learner or activity weighting. Valid zeros contribute and missing scores have no weight. |
| Content progress | Use distinct applicable published units visited divided by the published unit count. Repeated visits count once. Confirm visits satisfy the client metric or provide a completion feed. No verified navigation or no usable unit inventory leaves progress NULL. |
| Empty rosters and time scope | For no registered learners, retain the inventory total and leave completed/remaining NULL. Should an empty class instead have zero completed or vacuous completion? Confirm current registered rosters and latest published inventory for selected-period delivery, the CCX eligibility rule, and UTC as the reporting timezone. |

## Refresh and acceptance

The learner, activity, class and catalog-coverage tables use ClickHouse replacement materialized views with a four-hour interval. They sort by institution and CCX, plus the learner or block where applicable. One successful refresh replaces the whole result. `ref()` orders dbt creation. The physical `*_mv` dependencies order scheduled refreshes. Catalog coverage follows published course structure, delivery follows catalog coverage and attempt/score inputs, and class metrics follow learner progress, delivery and practice subdomains. `pss_reporting_refresh_dependencies` can override the upstream physical MV names per `catalog`, `learner`, `delivery` and `class` after inspecting the deployed graph. Include refreshable coverage producers in these lists if applicable. Input lag and refresh failures can extend age beyond four hours.

1. After the upstream changes land, verify their real schemas, MVs, taxonomy, policy and coverage. Enable `pss_reporting_enabled: true` in staging and build the upstreams before `dbt build --select tag:pss_reporting`. Use the isolated fixture checks for development on the main baseline.
2. Check `system.view_refreshes` for dependency names, last successful refresh, next refresh and exceptions. Compare input watermarks with roster/publication changes. Verify at least two successive scheduled cycles, deterministic results on unchanged inputs and representative-volume refresh time, query latency and memory. Agree a freshness limit with operations.
3. Reconcile institution/class/learner keys, instructor grants and revocations, LMS roster and grades, visits, activity order, practice subdomains, final provider evidence and pass rates in staging under identical scopes and dates. Include classes with no attempts, missing coverage and incomplete catalogs. Record existing Course Licensing and MeasureUp regression counts.
4. Production requires staging reconciliation, metric approval and release approval. Repeat scoped access, population, KPI and freshness checks on the approved production release. Link both environments' evidence and release revisions to PADV-3837.
5. For a rollback, withdraw the affected dataset references and restore the prior deployment revision. Disabling a dbt model leaves its existing ClickHouse objects in place. Stop or remove only the four PADV-3837 refreshable views and their owned targets in downstream-first order using the approved operational process. Recreate only affected targets for later column or ordering changes.

For a local fixture build, use the repository's Python 3.12 development environment, the pinned dbt packages and a ClickHouse 25.8 executable. The fixture starts a private loopback server, builds the real report graph against synthetic upstream rows and refreshes all four reporting tables twice.

```bash
dbt deps
git fetch origin pull/21/head
CLICKHOUSE=/path/to/clickhouse python integration_tests/pss_reporting/run.py
```
