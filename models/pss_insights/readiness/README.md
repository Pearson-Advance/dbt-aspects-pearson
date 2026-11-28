# PSS learner readiness and weaknesses

Use the PSS readiness score to explain a registered learner's standing in one CCX. Use the complete practice and Quiz detail to identify specific objectives for review. The separate MeasureUp enrollment status is available from `rpt_measureup_exam_readiness`. [Scores and attempts](../scores/README.md)

## Reporting contracts

| Relation | Stable grain | What to consume |
| --- | --- | --- |
| `rpt_pss_learner_readiness_current` | One registered `(ccx_id, actor_id)` | Nullable `readiness_score` on 0–1, `readiness_status`, `readiness_unavailable_reason`, contributing weight and each type's status, score, policy weight, sitting and activity counts. |
| `rpt_pss_practice_subdomain_results` | One MeasureUp `attempt_key` × provider domain/subdomain ID pair, including an unattributed bucket | UTC attempt start, original attempt and test IDs, outcome counts, coverage, ratio and weak flag. Read all rows for a sitting. The observed counts aid investigation when final counts are unavailable. |
| `rpt_pss_learner_practice_weaknesses_current` | One registered `(ccx_id, actor_id)` | `ranking_status` and up to three tuples of domain ID, subdomain ID, label, pooled correct, pooled total and score. Inspect the status even when the array is empty. |
| `rpt_pss_quiz_subdomain_results` | Scoped learner and Quiz sitting × provider/taxonomy/domain/subdomain | Allocated observed numerator and denominator, distinct item count, nullable score and weak flag, version, rule and coverage. NULL IDs preserve domain-only, unmapped or unverified evidence. |

Each output carries institution, CCX and master course. Filter on authorized **institution and CCX pairs** before aggregating or returning learner records. Instructor assignments can produce multiple grants for one class. Use membership in the authorized set to avoid multiplying rows. One actor registered in two classes has two independent memberships. For authenticated principal lookup and restricted relations, use the [class access contract](../context/README.md).

## Reproducing a score

1. Use the score models' sitting identity and reviewed participation rule for Quiz and Lab, and its practice eligibility flag for Practice Exam. Exclude Final Exam. For Quiz/Lab, use the selected current activity score for the same learner, class and activity. A native LMS grade alone cannot establish a dated, eligible sitting.
2. Pool eligible practice questions as `sum(correct_items) / sum(total_items)`. A known question with unknown objective can still contribute to whole-attempt performance. An unresolved denominator blocks the contribution. For a practice subdomain, pool the same two counts across eligible sittings. Return scores below 0.70 in ascending order, breaking ties by stable provider subdomain ID. Separate database identities in different test versions remain separate even if their labels coincide.
3. Multiply each participating type's fraction by its current course weight, sum the products and divide by the sum of those weights. A zero-score attempted Lab enters both the calculation and the denominator. A verified unattempted Lab contributes no weight. Compare the unrounded result with 0.70 (On Target) and 0.60 (Borderline). Lower calculated scores are At Risk. Missing evidence yields NULL score and band with an explicit reason.

For Quiz/Lab/Practice/Final weights 20/30/40/10, a 60% Quiz and practice scores of 1/2 and 8/10 give a practice fraction of 9/12. Weighted readiness is 42/60 = **0.70, On Target**. An eligible zero-score Lab changes it to 42/90, **At Risk**. Display rounding must happen after band assignment.

## Selected periods

Pass the authorized class relation and SQL expressions for both UTC bounds into `pss_learner_readiness` or `pss_practice_weaknesses_for_period`. For example, the following returns the results for one UTC day. In this example, `authorized_classes` is a relation supplied by the caller and contains the authenticated principal’s class context access rows. The [scoped replay model](../../../integration_tests/pss_readiness/fixture_models/fixture_period_readiness.sql) shows the same call with a dbt relation.

```sql
{{ pss_learner_readiness(
    "authorized_classes",
    "toDateTime64('2026-11-02 00:00:00', 6, 'UTC')",
    "toDateTime64('2026-11-03 00:00:00', 6, 'UTC')"
) }}
```

The lower bound is inclusive and the upper bound exclusive. Both queries recompute from dated sittings using the current roster, course policy and latest surviving source corrections. The start selects a sitting. Under the latest-finished rule, the latest eligible finish before the exclusive end can supply the score. When the available history cannot establish a score by that time, the affected contribution is unavailable. For chart-level Quiz or practice attempt detail, scope the same institution/CCX pairs before filtering `attempted_at` by the same bounds. This selects sittings by start time and returns their latest corrected detail. Study-period Quiz outcome timestamps require their own source evidence. Missing attempt times cannot establish absence in a window. Current-only LMS results may yield `period_score_history_unavailable`. Coverage, freshness and roster membership must accompany period comparisons. A historical roster or policy reconstruction requires separately versioned inputs.

## Evidence and decisions

| Owner | Decision and current behavior |
| --- | --- |
| PSS Product and scoring owner | Choose the Quiz/Lab retake and activity score rule per master through the [score configuration](../scores/README.md). Add a master to `pss_readiness_mean_activity_masters` to approve equal weighting of different activity scores within a type. Missing eligible scores or weights leave the affected readiness unavailable. |
| Product and provider data owner | Finished sittings with unambiguous binary answers can be counted. Confirm how partial credit, incomplete sittings and unanswered items enter the denominator. `pss_practice_item_rules` accepts `scored_only` or `all_exported` per approved master. Both count full credit at 1. The former excludes unscored/unanswered items, the latter counts them incorrect. Known invalid values and contradictory linkage stay unresolved. |
| Content, Product and Data | Choose the Quiz scoring unit and multi-tag allocation per master in `pss_quiz_weakness_rules`. Native `equal_points` splits earned and possible points equally over distinct objectives after removing redundant parents. MeasureUp `binary_0_1` accepts answered binary results on a verified provider version. Domain-only scores retain the domain. Approve a historical mapping and a separate crosswalk before comparing Studio and provider objectives. |

The shipped rules and evidence inputs carry no production approvals. The score models provide independently reconciled intervals for applicable activities. An operator can also supply `pss_readiness_history_coverage_model` with the schema of the header-only `pss_readiness_history_coverage` seed. A row identifies the institution, CCX, actor, current publication, type, UTC bounds, verification time, expiry and evidence reference. Two blank bounds assert an independently reconciled history with ongoing coverage until expiry. Retract the row on coverage loss. Bounded evidence must include the whole requested interval. Validate all participating feeds, eligibility and roster resolution before publishing evidence. A missing or stale certificate leaves telemetry unverified.

Native xAPI identifies an evaluated problem and statement. To attribute it to a whole sitting, a deployment can provide `pss_native_quiz_context_model` with the scoped actor/CCX, source statement, authentic sitting key and start, original problem and subsection, publication, evidence and mapping references, and the objective ID tuples known at the attempt. Current tags alone cannot establish which tags applied then. Unlinked or contradictory source claims remain in the restricted attribution model. The optional adapter and tested input columns are documented in the [Quiz model schema](quiz/_quiz__models.yml).

## Refresh and verification

The reporting tables and shared readiness evidence use four-hour replacement refreshes. Source corrections and tombstones enter the next successful dependency chain. The practice results follow the attempt materialization. The practice ranking reads the practice detail and attempts. The Quiz result waits for the attempt, native/provider item and course-block materializations. The readiness evidence waits for current activity scores, attempts and practice detail. Current learner readiness follows that evidence and course structure. Read `system.view_refreshes` for actual last success, duration, rows and failures. The four-hour schedule describes cadence. Source export delays and upstream refresh failures affect observed freshness.

Run the replay from this repository with Docker, the pinned dbt packages, local source directories for the deployment plugin containing the assignment type table DDL and the Aspects 3.0.3 migrations.

```sh
dbt deps
python integration_tests/pss_readiness/run.py \
  --tutor-pearson /path/to/deployment-plugin \
  --tutor-aspects /path/to/analytics-plugin
```

The fixture creates a disposable ClickHouse and builds the actual dbt graph. It exercises the interval and authorization entry points, provider readiness, and repeated replacement refreshes. Fixture-only models live outside the production dbt project paths.

- [ ] On staging, reconcile at least one learner's original sitting, eligible question counts, weight, band and ordered weaknesses. Compare full objective row counts, check a second class with different access, and record source watermarks, last successful dependent refresh and representative runtime.
- [ ] Before class reporting and the dashboard datasets consume these models, check unavailable coverage end to end. Class weakest-subdomain consumers must propagate unresolved objective/denominator evidence before pooling. Restrict access to the underlying tables and restricted diagnostics.
- [ ] After staging acceptance and release approval, repeat the scoped checks for an approved production class. For rollback, revert consumers to their prior definitions, remove the readiness refreshable views in reverse dependency order, then their targets and auxiliary views. Preserve the shared upstream contract and producer records.
