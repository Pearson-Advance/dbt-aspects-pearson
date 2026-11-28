# PSS published activity inventory

This inventory supplies the activities an instructor can report before any learner attempts them. The class context supplies the eligible class, institution, license and master keys. Both are current reporting contracts. [Class context](../context/README.md)

| Output | One row per | Usage |
| --- | --- | --- |
| `int_pss_activity_catalog` | Eligible CCX and recognized, applicable graded master subsection | Join attempts to the exact class usage key. Filter `in_latest_publish` and order by `activity_sequence` for a current class. Historical rows have NULL sequence. Current rows remain while the latest publication awaits verification; check the class inventory coverage before treating the list as complete. |
| `int_pss_activity_type_coverage` | In-scope master and observed graded subsection | Inspect raw Studio assignment type, mapping status, component counts and publication flag. Counts come from the captured revision when available. |
| `int_pss_master_inventory_coverage`, `int_pss_class_inventory_coverage` | Master or CCX, respectively | Inspect `publication_status`, `inventory_status` and, for a class, `applicability_status`. Only `classified` and `no_graded_subsections` establish a complete class inventory. |
| `int_pss_grading_policy`, `int_pss_grading_policy_coverage` | Master and type, or one master for coverage | Only a valid four-row policy supplies weights and an exam pass rule. Coverage distinguishes configured, missing and invalid policies. |
| `int_pss_course_tag_captures` | Master, UTC export time and problem block key | An empty block key marks the course export, including one with zero problems. The problem rows retain the observed tag IDs, evidence status and dump ID. |
| `int_pss_quiz_problem_objectives` | Master, UTC export time and problem block key | Resolves the observed tag IDs against the approved taxonomy scope and the tag dictionary available at export time. Native Quiz results use this history. |
| `int_pss_quiz_item_subdomain_map` | Master problem, taxonomy and distinct domain/subdomain assignment | Use `in_latest_publish` and `mapping_status = 'observed'` for complete current assignments. A parent-only mapping has a NULL subdomain. Deduplicate by problem before aggregating whole-item scores. |
| `int_pss_quiz_item_structure_coverage` | Current recognized Quiz subsection | Compares observed Aspects problem counts with the Studio snapshot. A mismatch withholds the affected current item mappings and master Quiz totals. |
| `int_pss_quiz_item_subdomain_coverage` | Scoped master | Counts current graded problems and the identified Quiz subset, including domain-only and unmapped items. Missing taxonomy configuration or dictionary evidence leaves affected mapping counts NULL. |

## Publication, order and history

The CMS source emits one row per complete attached master-course publication. Its immutable key is `(course_key, snapshot_id)`. An empty array is meaningful because the collector completes the traversal before sending anything. The source timestamp orders revisions. An asynchronous delivery or backfill can arrive later. Only a matching live head supplies `head_confirmed_at`. An observed newer CourseOverview or disagreement with available Aspects subsection keys and graded flags makes current inventory unavailable until both feeds reconcile. The class and master coverage rows survive missing or conflicting inputs.

The catalog projects the applicable class curriculum from verified scope and type mappings. Each graded subsection takes the raw **Graded as** value of that published subsection. A MeasureUp launch inherits its containing subsection’s type. For example, a mapped lesson Quiz and a full Practice Exam can contain the same provider component. Quiz objective tags enrich problems separately.

Removed activities can remain linkable under the last observed applicable publication. Historical sequence is NULL. Policy and taxonomy lookups use current configuration. The Studio revision and the Aspects structure dump have different identifiers. Replacing a problem with another in the same subsection can preserve its problem count. Reconcile the structure and Studio revision at the same cutoff before certifying item coverage. Past first publication, historical CCX overrides, missed source revisions and item versions cannot be reconstructed from an initial backfill.

## Configuration and ownership

The reviewed seeds ship with headers. Content and Product supply real mappings or weights through review. No default activity type is inferred for Studio names such as Homework.

| Input | Key and change process |
| --- | --- |
| `pss_assignment_type_mapping.csv` | One exact Studio assignment-type name per master-course run maps to Quiz, Lab, Practice Exam or Final Exam. Update it when Content changes a name. |
| `pss_grading_policy.csv` | Four rows per configured master. Weights are percentages from 0 to 100 and total 100. A genuine zero is valid. A native Final Exam has `native_pass_mark` and its percentage. A MeasureUp final has `measureup_cut_score` and uses the attempted version’s cut score from the score models. Only the Final Exam row can specify a pass rule. |
| `pss_curriculum_applicability.csv` or `pss_class_applicability_model` | Platform supplies evidence of full inheritance, an exact class/revision subset, or an explicitly empty class curriculum. A dbt relation with the seed's columns can supply this programmatically. See the rules below. |
| `pss_quiz_taxonomies_by_master` or `PSS_QUIZ_TAXONOMIES_BY_MASTER` | Per-environment mapping from exact master-course run to a list of approved integer taxonomy IDs. For example, `{"course-v1:EXAMPLE+SEC+RUN":[30]}` illustrates the shape. Configure the approved IDs for each master and run. Tagged LTI launches await a separate decision. |

For `inherit_master`, a blank CCX limits the rule by master and a blank snapshot extends it across revisions. Use that scope only where inheritance has been verified at that level. For `include_subsection`, supply one row per applicable subsection in the complete set for the exact class and snapshot. Use `empty` for a verified empty set for that class and snapshot. A class override takes precedence. Stale class-specific evidence cannot fall back to broader master inheritance. A missing approval leaves the class visible in coverage.

Each Aspects course export records the problem's full tag array in `event_sink.course_blocks.xblock_data_json.tags`. The current map uses the latest observed course export. Native Quiz attribution uses the latest course export at or before the xAPI response timestamp, then looks for the problem in that specific dump. This order prevents an older mapping from reappearing after the problem is removed. A valid empty array establishes that the problem had no tags at that observation. A missing problem, malformed array or conflicting export leaves the mapping unverified. Individual `object_tag` records can outlive their assignments.

The observation clock is `time_last_dumped` in UTC. A tag edit between exports becomes visible when Aspects next exports the course structure. Changes made and reverted between exports can go unobserved. For reconciliation, record the class, master, problem, dump ID and export time, then compare Studio and ClickHouse at the same cutoff.

## Build, verify and release

1. Install the class-context models and the Tutor table DDL before building the inventory. The dbt views reference the source even when collection is disabled. Once collection is approved, install the collector on Studio and CMS workers, configure the approved masters, then enable and backfill those masters. Measure the delay from publication through queue delivery and the independent `dim_course_blocks` refresh.
2. Load the approved seeds or applicability relation, then run `dbt seed --select tag:pss_insights`, `dbt run --select +tag:pss_insights` and `dbt build --select tag:pss_insights`. Inspect policy, class and master coverage after every reconfiguration. ClickHouse refreshes the capture history every four hours and rebuilds the resolved objectives after the captures. The current map reads the refreshed observations. Source delivery and the last successful dependent refresh determine freshness. Rebuild the affected models when changing dbt vars.
3. To replay the synthetic acceptance cases, install the dbt project dependencies, run `dbt deps`, then run the following command with Docker available. Supply local source directories for the deployment plugin containing the assignment DDL and the Aspects 3.0.3 migrations. The replay provisions and removes its own ClickHouse 25.8 container, uses the real class context models, and runs both default and configured builds.

   ```sh
   python integration_tests/pss_inventory/replay.py \
     --tutor-pearson /path/to/deployment-plugin \
     --tutor-aspects /path/to/analytics-plugin
   ```

4. In staging, record the app, dbt, source and configuration revisions, UTC cutoff and `snapshot_id`. Compare the independently exported class/master/institution/license keys against the class context. Compare published Studio subsections and class overrides to ordered catalog keys, raw types and coverage. Reconcile one unmapped type, a removed activity, a genuinely empty publication, an unobserved class, a zero weight, both final rules, and a tagged/retagged/untagged Quiz problem. Record discrepancies and current counts under the approved scope.
5. The score models consume class activity keys and the final-rule reference. The readiness models consume current policy and item-to-objective assignments. Class reporting must use the **class** inventory coverage to distinguish an unavailable or partial class curriculum from a verified zero. The reporting consumer also needs to preserve authorized class scope.
6. Production reconciliation requires accepted staging evidence and release approval. For rollback, stop dependent publication, revert the dbt package and its configuration to the previously approved revision, and redeploy the affected views. Disable the collector and its Waffle flag or clear its course scope on Studio and workers. Retain the source table and prior observations for reconciliation.
