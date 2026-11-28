"""Replay class reporting from Ulmo source records using the complete PSS dbt stack."""

from pathlib import Path
import sys
import tempfile

import yaml

HERE = Path(__file__).resolve().parent
sys.path[:0] = [str(HERE.parent), str(HERE.parent / "pss_readiness")]
from pss_readiness.run import Fixture, database, provision, refresh
from disposable import schema_paths
from inputs import (
    ACTOR,
    CLASS_A,
    CLASS_B,
    OTHER,
    UNKNOWN,
    configure_readiness,
    load_source_rows,
)
from quiz import prepare_quiz, load_quiz_events
from sources import START, sink
from source_rows import (
    EMPTY,
    REVISED,
    certify,
    coach_a_grant,
    extend_course,
    lms_events,
    provider_sitting,
)

CLASSES = "reporting.rpt_pss_class_metrics"
LEARNERS = "reporting.rpt_pss_learner_progress"
DELIVERY = "reporting.rpt_pss_class_activity_delivery"
MASTER_SUMMARY = "reporting.fixture_reporting_master"
PERIOD = "reporting.fixture_reporting_period"


def verify_refresh_edges(f):
    edges = {
        "rpt_pss_learner_progress": [
            "rpt_pss_activity_attempts",
            "rpt_pss_activity_scores_current",
            "rpt_pss_learner_readiness_current",
        ],
        "rpt_pss_class_metrics": [
            "rpt_pss_learner_progress",
            "rpt_pss_class_activity_delivery",
            "rpt_pss_practice_subdomain_results",
        ],
        "rpt_pss_class_activity_delivery": [
            "rpt_pss_activity_attempts",
            "rpt_pss_activity_scores_current",
        ],
    }
    for model, parents in edges.items():
        ddl = f.sql(f"SHOW CREATE TABLE reporting.{model}_mv FORMAT TSVRaw").replace(
            "`", ""
        )
        dependencies = ddl.split("DEPENDS ON ", 1)[1].split(" TO ", 1)[0]
        required = ["reporting." + parent + "_mv" for parent in parents]
        required.append(
            "reporting_pss_restricted.int_pss_reporting_unlinked_attempts_mv"
        )
        for parent in required:
            if parent not in dependencies:
                raise AssertionError(f"Missing scheduled edge {model} -> {parent}")
    print(
        "PASS physical dependencies on the reporting and readiness tables", flush=True
    )


def check_metrics(f):
    f.expect(
        "one row per eligible class including the empty class",
        f"SELECT count(),uniqExact(ccx_id) FROM {CLASSES}",
        "3\t3",
    )
    f.expect(
        "distinct registered membership grain",
        f"SELECT count(),uniqExact((ccx_id,actor_id)) FROM {LEARNERS}",
        "6\t6",
    )
    f.expect(
        "every listed activity remains unique including unopened activities",
        f"SELECT count(),uniqExact((ccx_id,subsection_block_id)) FROM {DELIVERY}",
        "21\t21",
    )
    f.expect(
        "boundaries and practice evidence from readiness",
        f"SELECT readiness_status, readiness_score FROM {LEARNERS} WHERE ccx_id='{CLASS_A}' AND actor_id='{ACTOR}'",
        "On Target\t0.7",
    )
    f.expect(
        "class readiness, support reasons and full practice objective detail",
        f"SELECT on_target,at_risk,readiness_unavailable,class_risk, support_reasons, weakest_practice_subdomain_id, weakest_practice_subdomain_score FROM {CLASSES} WHERE ccx_id='{CLASS_A}'",
        "1\t1\t0\tHigh\t['Quiz','Practice Exam']\t511\t0.4",
    )
    f.expect(
        "LMS grade zero participates and duplicate unit visits count once",
        f"SELECT avg_course_grade, course_grade_learners FROM {CLASSES} WHERE ccx_id='{CLASS_A}'",
        "0.4\t2",
    )
    f.expect(
        "a repeated visit to one published unit",
        f"SELECT published_unit_count,observed_visited_unit_count,visited_unit_count,abs(content_progress-1/7)<1e-10 FROM {LEARNERS} WHERE ccx_id='{CLASS_A}' AND actor_id='{ACTOR}'",
        "7\t1\t1\t1",
    )
    f.expect(
        "a missing LMS score remains unknown",
        f"SELECT isNull(course_grade) FROM {LEARNERS} WHERE ccx_id='{CLASS_B}' AND actor_id='{UNKNOWN}'",
        "1",
    )
    f.expect(
        "distinct people passed across unequal active classes and retakes",
        f"SELECT active_classes,registered_learners,final_attempted_learners,final_passed_learners,final_pass_rate FROM {MASTER_SUMMARY}",
        "2\t4\t4\t2\t0.5",
    )
    f.expect(
        "simple averaging would have returned 62.5 percent",
        f"SELECT avg(final_pass_rate) FROM {CLASSES} WHERE ccx_id IN ('{CLASS_A}','{CLASS_B}')",
        "0.625",
    )
    f.expect(
        "unavailable readiness has no class risk",
        f"SELECT isNull(class_risk),isNull(pct_on_target) FROM {CLASSES} WHERE ccx_id='{CLASS_B}'",
        "1\t1",
    )
    f.expect(
        "the zero-roster class retains its published inventory",
        f"SELECT registered_learners,total_activities,isNull(completed_activities),isNull(remaining_activities) FROM {CLASSES} WHERE ccx_id='{EMPTY}'",
        "0\t7\t1\t1",
    )
    f.expect(
        "all-history versus open activities",
        f"SELECT completed_activities,total_activities,remaining_activities FROM {CLASSES} WHERE ccx_id='{CLASS_A}'",
        "2\t7\t5",
    )
    f.expect(
        "date-selected delivery excludes later sittings and retains unopened Lab",
        f"SELECT count(),countIf(delivery_complete=1),countIf(activity_type='Lab' AND attempted_learners=0) FROM {PERIOD} WHERE ccx_id='{CLASS_A}'",
        "7\t1\t1",
    )
    f.expect(
        "later authorized period returns verified zeros for all types",
        f"SELECT sum(completed_activities),sum(total_activities),sum(remaining_activities) FROM reporting.fixture_reporting_later WHERE ccx_id='{CLASS_A}'",
        "0\t7\t7",
    )
    f.expect(
        "period report excludes a class in a different institution",
        f"SELECT countIf(ccx_id='{EMPTY}') FROM {PERIOD}",
        "0",
    )
    f.expect(
        "published order determines Quiz difficulty",
        f"SELECT arrayStringConcat(groupArray(section),',') FROM (SELECT subsection_name AS section FROM {DELIVERY} WHERE ccx_id='{CLASS_A}' AND activity_type='Quiz' ORDER BY activity_sequence)",
        "quiz,ten,zero,missing",
    )


def verify_missing_evidence(f):
    # Each withdrawal affects only the disposable source ledgers. The published
    # Lab remains in the inventory across every state.
    f.sql("TRUNCATE TABLE reporting.pss_activity_history_coverage")
    refresh(f)
    current = f"SELECT observed_attempted_learners,isNull(attempted_learners),isNull(delivery_complete) FROM {DELIVERY} WHERE ccx_id='{CLASS_B}' AND activity_type='Lab'"
    f.expect(
        "unverified all-history Lab preserves its observed zero", current, "0\t1\t1"
    )
    period = f"SELECT count(),countIf(attempted_learners=0),countIf(isNull(attempted_learners)) FROM {PERIOD} WHERE ccx_id='{CLASS_B}' AND activity_type='Lab'"
    f.expect(
        "bounded reconciliation still certifies an unopened Lab", period, "1\t1\t0"
    )
    # A persisted source sitting with an unknown logical activity could belong
    # to the Lab or Final. It invalidates an otherwise current reconciliation.
    provider_sitting(f, 904, 43, CLASS_B, "unmapped", 50, START)
    refresh(f)
    f.expect("unattributed sitting blocks a definitive period count", period, "1\t0\t1")
    f.expect(
        "unattributed sitting blocks an otherwise certified final rate",
        f"SELECT isNull(final_pass_rate) FROM {CLASSES} WHERE ccx_id='{CLASS_B}'",
        "1",
    )
    f.expect(
        "a disputed sitting affects only its owner's final history",
        f"SELECT actor_id,final_history_complete,ifNull(toString(final_passed),'unknown') FROM {LEARNERS} WHERE ccx_id='{CLASS_B}' AND actor_id IN ('{ACTOR}','{OTHER}') ORDER BY actor_id",
        f"{ACTOR}\t1\t0\n{OTHER}\t0\tunknown",
    )


def report_runtime(f):
    f.sql(
        f"/* PSS reporting scoped summary */ SELECT active_classes,final_pass_rate FROM {MASTER_SUMMARY}"
    )
    f.sql(
        f"/* PSS reporting scoped period */ SELECT count(),sum(observed_attempted_learners) FROM {PERIOD}"
    )
    f.sql("SYSTEM FLUSH LOGS")
    print(
        f.sql(
            "SELECT extract(query,'scoped (summary|period)') AS consumer,query_duration_ms,read_rows,memory_usage FROM system.query_log WHERE type='QueryFinish' AND startsWith(query,'/* PSS reporting scoped ') ORDER BY consumer FORMAT PrettyCompact"
        ),
        flush=True,
    )
    print(
        f.sql(
            "SELECT view,last_success_time,last_success_duration_ms,read_rows,written_rows FROM system.view_refreshes WHERE view IN ('rpt_pss_class_metrics_mv','rpt_pss_learner_progress_mv','rpt_pss_class_activity_delivery_mv') ORDER BY view FORMAT PrettyCompact"
        ),
        flush=True,
    )


def exercise(f):
    load_source_rows(f)
    extend_course(f)
    prepare_quiz(f)
    f.dbt("seed", "--select", "tag:pss_insights")
    configure_readiness(f)
    certify(f)
    f.dbt(
        "run",
        "--select",
        "+tag:pss_insights",
        "+tag:pss_reporting",
        "+tag:pss_context",
        "+tag:pss_scores",
        "+tag:pss_readiness",
        "+fixture_reporting_master",
        "+fixture_reporting_period",
        "+fixture_reporting_later",
        "+rpt_measureup_exam_readiness",
    )
    lms_events(f)
    load_quiz_events(f)
    verify_refresh_edges(f)
    refresh(f)
    check_metrics(f)
    provider_sitting(f, 201, 42, CLASS_A, "quiz", 20, START, REVISED)
    refresh(f)
    f.expect(
        "a corrected source score flows through attempts, readiness, learner, class and difficulty",
        f"SELECT on_target, at_risk, round(avg_quiz_score,2), (SELECT avg_score_pct FROM {DELIVERY} WHERE ccx_id='{CLASS_A}' AND subsection_name='quiz') FROM {CLASSES} WHERE ccx_id='{CLASS_A}'",
        "0\t2\t0.35\t35",
    )
    f.expect(
        "unrelated provider status survives correction",
        "SELECT status,last_score FROM reporting.rpt_measureup_exam_readiness",
        "fixture-provider-status\t0.97",
    )
    reports = [LEARNERS, DELIVERY, CLASSES]
    queries = [
        f"SELECT * FROM {report} ORDER BY ALL FORMAT JSONEachRow" for report in reports
    ]
    prior = [f.sql(q) for q in queries]
    for cycle in (1, 2):
        refresh(f)
        for report, query, expected in zip(reports, queries, prior):
            f.expect(f"repeat refresh {cycle} {report}", query, expected)
    coach_a_grant(f, revoked=True)
    f.expect(
        "revoking A drops its pass immediately for this principal",
        f"SELECT active_classes, registered_learners, final_attempted_learners, final_passed_learners,final_pass_rate FROM {MASTER_SUMMARY}",
        "1\t4\t4\t1\t0.25",
    )
    # A question in a counted sitting disappears from the verified objective catalog.
    sink(
        f,
        "measureup_question",
        [
            dict(
                source_id="51000",
                external_id="Q51000",
                name="Q51000",
                exam_subdomain_id="510",
            )
        ],
        REVISED,
        deleted=True,
    )
    refresh(f)
    f.expect(
        "a missing objective withholds the class ranking",
        f"SELECT isNull(weakest_practice_subdomain_id), isNull(weakest_practice_subdomain_score) FROM {CLASSES} WHERE ccx_id='{CLASS_A}'",
        "1\t1",
    )
    verify_missing_evidence(f)
    f.dbt(
        "test",
        "--select",
        "tag:pss_insights",
        "tag:pss_reporting",
        "tag:pss_context",
        "tag:pss_scores",
        "tag:pss_readiness",
        "fixture_period_readiness",
        "rpt_measureup_exam_readiness",
    )
    report_runtime(f)


def main():
    args = schema_paths(__doc__)
    with database(
        args.tutor_pearson, args.tutor_aspects
    ) as port, tempfile.TemporaryDirectory() as scratch:
        f = Fixture(port, Path(scratch))
        (f.project / "reporting_fixtures").symlink_to(HERE / "fixture_models")
        path = f.project / "dbt_project.yml"
        config = yaml.safe_load(path.read_text())
        config["model-paths"].append("reporting_fixtures")
        path.write_text(yaml.safe_dump(config, sort_keys=False))
        provision(f, args.tutor_pearson, args.tutor_aspects)
        exercise(f)


if __name__ == "__main__":
    main()
