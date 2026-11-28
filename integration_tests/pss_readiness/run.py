"""Replay readiness from synthetic Ulmo producer records on disposable ClickHouse."""

from graphlib import TopologicalSorter
from pathlib import Path
import re
import sys
import tempfile

import yaml

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [
    str(ROOT / "integration_tests"),
    str(ROOT / "integration_tests/pss_scores"),
]
from pss_scores.replay import Fixture as ScoresFixture, database, provision
from disposable import schema_paths
from inputs import ACTOR, CLASS_A, CLASS_B, MASTER, OTHER, UNKNOWN
from inputs import configure_readiness, load_source_rows, sitting
from quiz import prepare_quiz, load_quiz_events, verify_quiz
from native import EARNED, statement


class Fixture(ScoresFixture):
    """Expose authorized period queries and the reviewed native adapter only in this project."""

    def __init__(self, port, scratch):
        super().__init__(port, scratch)
        self.project = scratch / "project"
        self.project.mkdir()
        for item in (
            "models",
            "macros",
            "analyses",
            "tests",
            "seeds",
            "dbt_packages",
            "packages.yml",
            "package-lock.yml",
            "selectors.yml",
        ):
            (self.project / item).symlink_to(ROOT / item)
        (self.project / "fixture_models").symlink_to(
            Path(__file__).resolve().parent / "fixture_models"
        )
        project = yaml.safe_load((ROOT / "dbt_project.yml").read_text())
        project["model-paths"].append("fixture_models")
        (self.project / "dbt_project.yml").write_text(
            yaml.safe_dump(project, sort_keys=False)
        )

    def dbt(self, command, *flags, extra_vars=None):
        rules = {
            "pss_current_score_rules": {
                MASTER: {
                    "Quiz": "measureup_latest_finished",
                    "Lab": "measureup_latest_finished",
                }
            },
            "pss_native_quiz_context_model": "fixture_native_quiz_context",
            "pss_quiz_weakness_rules": {
                MASTER: {"native": "equal_points", "measureup": "binary_0_1"}
            },
            "pss_readiness_mean_activity_masters": [MASTER],
        }
        return super().dbt(
            command,
            *flags,
            "--project-dir",
            str(self.project),
            extra_vars={**rules, **(extra_vars or {})},
        )


def refresh(f):
    views = f.sql(
        "SELECT concat(database,'.',view) FROM system.view_refreshes WHERE startsWith(database,'reporting')"
    ).splitlines()
    # ClickHouse 25.8 records physical REFRESH ... DEPENDS ON edges in the DDL.
    graph = {}
    for name in views:
        ddl = f.sql("SHOW CREATE TABLE " + name + " FORMAT TSVRaw")
        identifier = r"(?:`[^`]+`|\w+)\.(?:`[^`]+`|\w+)"
        dependencies = re.search(
            r"\bDEPENDS ON\s+(" + identifier + "(?:,\s*" + identifier + ")*)", ddl
        )
        graph[name] = (
            [edge.strip().replace("`", "") for edge in dependencies[1].split(",")]
            if dependencies
            else []
        )
    for model, upstream in [
        (READINESS, "reporting.int_pss_readiness_evidence"),
        (WEAKNESSES, PRACTICE),
        (PRACTICE, "reporting.rpt_pss_activity_attempts"),
        (QUIZ, "reporting_pss_restricted.int_pss_native_item_evidence"),
    ]:
        if upstream + "_mv" not in graph[model + "_mv"]:
            raise AssertionError(
                f"Missing physical refresh dependency {model} -> {upstream}: {graph[model + '_mv']}"
            )
    for name in TopologicalSorter(graph).static_order():
        f.sql("SYSTEM REFRESH VIEW " + name)
        f.sql("SYSTEM WAIT VIEW " + name)


READINESS = "reporting.rpt_pss_learner_readiness_current"
WEAKNESSES = "reporting.rpt_pss_learner_practice_weaknesses_current"
PRACTICE = "reporting.rpt_pss_practice_subdomain_results"
QUIZ = "reporting.rpt_pss_quiz_subdomain_results"


def verify_readiness(f):
    f.expect(
        "one row per registered learner and CCX",
        f"SELECT count(), uniqExact((ccx_id,actor_id)) FROM {READINESS}",
        "4\t4",
    )
    f.expect(
        "verified no activity and unavailable telemetry remain distinct",
        f"SELECT actor_id, isNull(readiness_score), readiness_unavailable_reason FROM {READINESS} WHERE ccx_id='{CLASS_B}' ORDER BY actor_id",
        f"{ACTOR}\t1\tno_eligible_attempts\n{UNKNOWN}\t1\ttelemetry_unverified",
    )
    f.expect(
        "weighted 60 percent Quiz and nine of twelve practice items",
        f"SELECT readiness_status, round(readiness_score, 8), contributing_weight, observed_practice_correct_items, observed_practice_total_items FROM {READINESS} WHERE ccx_id='{CLASS_A}' AND actor_id='{ACTOR}'",
        "On Target\t0.7\t60\t9\t12",
    )
    f.expect(
        "authorized period restricts both memberships and duplicate grants",
        "SELECT count(), uniqExact(actor_id), uniqExact(ccx_id), any(ccx_id) FROM reporting.fixture_period_readiness",
        f"2\t2\t1\t{CLASS_A}",
    )
    f.expect(
        "only Quiz remains in the first hour",
        f"SELECT readiness_score, contributing_weight, quiz_sittings, practice_sittings FROM reporting.fixture_period_readiness WHERE actor_id='{ACTOR}'",
        "0.6\t20\t1\t0",
    )


def verify_practice(f):
    f.expect(
        "every objective survives the first eligible sitting",
        f"SELECT subdomain_name, correct_items,total_items,is_weak FROM {PRACTICE} WHERE exam_attempt_id='401' ORDER BY subdomain_id",
        "A\t1\t5\t1\nB\t2\t5\t1\nC\t2\t5\t1\nD\t3\t5\t1\nE\t7\t10\t0",
    )
    f.expect(
        "pool across sittings then sort ties by stable ID",
        f"SELECT ranking_status, arrayMap(x -> x.2,top_weaknesses),top_weaknesses[1].4,top_weaknesses[1].5 FROM {WEAKNESSES} WHERE ccx_id='{CLASS_A}' AND actor_id='{OTHER}'",
        "ranked\t['510','511','512']\t1\t10",
    )
    f.expect(
        "period ranking excludes the next sitting at the upper bound",
        f"SELECT ranking_status,arrayMap(x -> x.2,top_weaknesses),top_weaknesses[1].4,top_weaknesses[1].5 FROM reporting.fixture_period_weaknesses WHERE actor_id='{OTHER}'",
        "ranked\t['510','511','512']\t1\t5",
    )


def exercise(f):
    load_source_rows(f)
    prepare_quiz(f)
    f.dbt("seed", "--select", "tag:pss_insights", "tag:pss_scores", "tag:pss_readiness")
    configure_readiness(f)
    f.dbt(
        "run",
        "--select",
        "+tag:pss_readiness",
        "+tag:pss_scores",
        "+fixture_period_readiness",
        "+fixture_period_weaknesses",
        "+rpt_measureup_exam_readiness",
    )
    load_quiz_events(f)
    refresh(f)
    verify_readiness(f)
    verify_practice(f)
    verify_quiz(f)
    f.expect(
        "provider readiness retains its independent enrollment grain",
        "SELECT exam_ready_status_id,status,last_score FROM reporting.rpt_measureup_exam_readiness",
        "900\tfixture-provider-status\t0.97",
    )
    sitting(f, 202, "lab", 0)
    refresh(f)
    f.expect(
        "attempted zero Lab enters the denominator",
        f"SELECT readiness_status, abs(readiness_score-42/90)<1e-12, contributing_weight,lab_score FROM {READINESS} WHERE ccx_id='{CLASS_A}' AND actor_id='{ACTOR}'",
        "At Risk\t1\t90\t0",
    )
    queries = [
        "SELECT * FROM " + name + " ORDER BY ALL FORMAT JSONEachRow"
        for name in (READINESS, WEAKNESSES, PRACTICE, QUIZ)
    ]
    prior = [f.sql(query) for query in queries]
    for iteration in (1, 2):
        refresh(f)
        for query, expected in zip(queries, prior):
            f.expect(
                f"repeatable pass {iteration} " + query.split()[3], query, expected
            )
    # Only a later LMS snapshot survives for this additional Quiz. It cannot
    # establish the date or the grade of a sitting in the selected hour.
    f.insert(
        "xapi.xapi_events_all",
        [statement(34001, EARNED, "sequential", "ten", {"raw": 1, "max": 10})],
    )
    refresh(f)
    f.expect(
        "a current LMS grade cannot reconstruct an earlier period",
        f"SELECT isNull(readiness_score), isNull(readiness_status), quiz_status FROM reporting.fixture_period_readiness WHERE actor_id='{ACTOR}'",
        "1\t1\tperiod_score_history_unavailable",
    )
    f.dbt("build", "--select", "tag:pss_readiness", "--exclude", "resource_type:seed")
    f.dbt(
        "test",
        "--select",
        "tag:pss_scores",
        "int_pss_learner_class_context",
        "rpt_measureup_exam_readiness",
        "fixture_period_readiness",
    )
    f.dbt(
        "run",
        "--select",
        "int_pss_native_quiz_context",
        "int_pss_quiz_objective_items",
        "rpt_pss_quiz_subdomain_results",
        "--full-refresh",
        extra_vars={
            "pss_native_quiz_context_model": None,
            "pss_quiz_weakness_rules": {},
        },
    )
    refresh(f)
    f.expect(
        "unconfigured native evidence and unapproved provider rule",
        f"SELECT countIf(provider='native'), countIf(provider='measureup' AND has(item_statuses,'rule_unapproved')) > 0, countIf(isNotNull(score)) FROM {QUIZ}",
        "0\t1\t0",
    )
    # Withdraw each source of period attestation in the disposable database.
    f.sql("TRUNCATE TABLE reporting.pss_readiness_history_coverage")
    period_status = f"SELECT ranking_status, length(top_weaknesses) FROM reporting.fixture_period_weaknesses WHERE actor_id='{OTHER}'"
    f.expect(
        "activity-level reconciliation covers the practice window",
        period_status,
        "ranked\t3",
    )
    f.sql("TRUNCATE TABLE reporting.pss_attempt_coverage_evidence")
    f.expect(
        "uncertified practice period remains unavailable",
        period_status,
        "interval_unverified\t0",
    )
    print(
        f.sql(
            "SELECT view, status, last_success_duration_ms, read_rows, written_rows FROM system.view_refreshes "
            "WHERE position(view,'pss')>0 ORDER BY view FORMAT PrettyCompact"
        ),
        flush=True,
    )


def main():
    args = schema_paths(__doc__)
    with database(
        args.tutor_pearson, args.tutor_aspects
    ) as port, tempfile.TemporaryDirectory() as scratch:
        f = Fixture(port, Path(scratch))
        provision(f, args.tutor_pearson, args.tutor_aspects)
        exercise(f)


if __name__ == "__main__":
    main()
