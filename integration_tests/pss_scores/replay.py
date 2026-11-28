"""Replay the score models on isolated ClickHouse, using the Ulmo source schemas and dbt graph."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from urllib.error import HTTPError


ROOT = Path(__file__).resolve().parents[2]
# Reuse the activity inventory's ephemeral database and HTTP fixture, along with the
# same class/license/Studio inputs. This script always starts a fresh server.
sys.path.insert(0, str(ROOT / "integration_tests/pss_inventory"))
import replay as inventory
from disposable import database, schema_paths

from native import load_items, load_native, verify_items, verify_native
from period import verify_period
from sources import (
    CLASS_A,
    CLASS_B,
    CORRECTION,
    MASTER,
    REMOVAL,
    attempt,
    configure,
    load_attempts,
    load_scope,
    provision,
    verify_historical,
    sink,
)


class Fixture(inventory.Fixture):
    def dbt(self, command, *flags, quiz_score_rule=None, extra_vars=None):
        variables = {
            "pss_quiz_taxonomies_by_master": {MASTER: [30]},
            "pss_roster_reconciled_until_utc": "2099-01-01 00:00:00",
            "pss_period_start_utc": "2026-11-02 10:00:00",
            "pss_period_end_utc": "2026-11-02 11:00:00",
        }
        if quiz_score_rule:
            variables["pss_current_score_rules"] = {MASTER: {"Quiz": quiz_score_rule}}
        variables.update(extra_vars or {})
        subprocess.run(
            [
                "dbt",
                command,
                "--no-partial-parse",
                "--profiles-dir",
                str(self.scratch),
                "--target-path",
                str(self.scratch / "target"),
                "--vars",
                json.dumps(variables),
                *flags,
            ],
            cwd=ROOT,
            check=True,
            env={
                **os.environ,
                "ASPECTS_EVENT_SINK_DATABASE": "event_sink",
                "ASPECTS_XAPI_DATABASE": "xapi",
                "ASPECTS_RAW_XAPI_TABLE": "xapi_events_all",
                "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            },
        )

    def sql(self, statement):
        try:
            return super().sql(statement)
        except HTTPError as exc:
            raise RuntimeError(exc.read().decode()) from exc

    def insert(self, table, rows):
        try:
            super().insert(table, rows)
        except HTTPError as exc:
            raise RuntimeError(exc.read().decode()) from exc


def refresh(f):
    for mv in [
        "reporting.dim_course_blocks_mv",
        "reporting.rpt_measureup_attempts_mv",
        "reporting.rpt_measureup_question_performance_mv",
        "reporting.rpt_pss_activity_attempts_mv",
        "reporting_pss_restricted.int_pss_native_item_evidence_mv",
        "reporting_pss_restricted.int_pss_measureup_quiz_item_evidence_mv",
        "reporting.rpt_pss_activity_scores_current_mv",
    ]:
        f.sql("SYSTEM REFRESH VIEW " + mv)
        f.sql("SYSTEM WAIT VIEW " + mv)


def exercise(f):
    f.expect(
        "fresh isolated target",
        "SELECT count() FROM system.tables WHERE database='reporting'",
        "0",
    )
    load_scope(f)
    load_attempts(f)
    load_items(f)
    f.dbt("seed", "--select", "tag:pss_insights", "tag:pss_scores")
    configure(f)
    f.dbt(
        "run",
        "--select",
        "+tag:pss_scores",
        "+int_pss_quiz_item_subdomain_map",
        "+int_pss_instructor_class_access",
        "+rpt_measureup_attempts",
        "+rpt_measureup_question_performance",
    )
    load_native(f)
    refresh(f)
    report = "reporting.rpt_pss_activity_attempts"
    f.expect(
        "same learner is registered in both classes",
        "SELECT count() FROM reporting.int_pss_learner_class_context WHERE is_registered",
        "2",
    )
    f.expect(
        "instructor grants survive",
        "SELECT count() FROM reporting_pss_restricted.int_pss_instructor_class_access WHERE has_instructor_grant",
        "2",
    )
    f.expect(
        "no copied sittings in second CCX",
        f"SELECT count() FROM {report} WHERE ccx_id='{CLASS_B}'",
        "0",
    )
    f.expect(
        "full and duplicated input gives two separate tied sittings",
        f"SELECT exam_attempt_id, score_pct, attempt_number FROM {report} WHERE exam_attempt_id IN ('101','102') ORDER BY exam_attempt_id",
        "101\t60\t1\n102\t72\t2",
    )
    f.expect(
        "exact final versions and mode",
        f"SELECT exam_attempt_id,score_pct,cut_score,is_qualifying_final FROM {report} WHERE activity_type='Final Exam' ORDER BY exam_attempt_id",
        "101\t60\t75\t1\n102\t72\t75\t1\n103\t80\t85\t1\n104\t80\t\\N\t1\n105\t\\N\t75\t1\n106\t65\t75\t0",
    )
    f.expect(
        "only one registered learner across qualifying retakes",
        f"SELECT uniqExact(actor_id) FROM {report} WHERE is_qualifying_final",
        "1",
    )
    f.expect(
        "confirmed practice remains distinct from a final",
        f"SELECT exam_attempt_id, is_participation,is_eligible_practice,is_qualifying_final FROM {report} WHERE activity_type='Practice Exam'",
        "301\t1\t1\t0",
    )
    f.expect(
        "unresolved provider evidence retained",
        "SELECT issue FROM reporting_pss_restricted.int_pss_measureup_attempt_diagnostics ORDER BY issue",
        "launch_without_attempt\nmissing_class_registration\nmissing_launch\nunmapped_launch_block",
    )
    verify_items(f)
    verify_native(f)
    sink(f, "measureup_exam_test_attempt", [attempt(102, 11, "80")], CORRECTION)
    refresh(f)
    f.expect(
        "source correction keeps identity and ordinal",
        f"SELECT score_pct, attempt_number FROM {report} WHERE exam_attempt_id='102'",
        "80\t2",
    )
    verify_period(f)
    stable = [
        (name, f.sql("SELECT * FROM " + name + " ORDER BY ALL FORMAT JSONEachRow"))
        for name in [
            report,
            "reporting.rpt_pss_activity_scores_current",
            "reporting.rpt_pss_quiz_item_results",
        ]
    ]
    for _ in range(2):
        refresh(f)
        for name, content in stable:
            f.expect(
                "repeatable " + name,
                "SELECT * FROM " + name + " ORDER BY ALL FORMAT JSONEachRow",
                content,
            )
    sink(
        f,
        "measureup_exam_test_attempt",
        [attempt(101, 11, "60")],
        REMOVAL,
        deleted=True,
    )
    # Replay the older original row after the deletion to test ingestion order.
    sink(f, "measureup_exam_test_attempt", [attempt(101, 11, "60")])
    refresh(f)
    f.expect(
        "tombstoned sitting cannot be restored by an old replay",
        f"SELECT exam_attempt_id,score_pct,attempt_number FROM {report} WHERE exam_attempt_id IN ('101','102')",
        "102\t80\t1",
    )
    f.expect(
        "existing MeasureUp report agrees on surviving source attempts",
        "SELECT exam_attempt_id, final_score, attempt_number FROM reporting.rpt_measureup_attempts WHERE exam_attempt_id IN ('101','102')",
        "102\t80\t1",
    )
    for rule, expected in [
        ("lms_subsection", "native\t10"),
        ("measureup_latest_finished", "measureup\t70"),
    ]:
        f.dbt(
            "run",
            "--select",
            "rpt_pss_activity_scores_current",
            "--full-refresh",
            quiz_score_rule=rule,
        )
        view = "reporting.rpt_pss_activity_scores_current_mv"
        f.sql("SYSTEM REFRESH VIEW " + view)
        f.sql("SYSTEM WAIT VIEW " + view)
        f.expect(
            "reviewed Quiz score rule " + rule,
            "SELECT score_source, score_pct FROM reporting.rpt_pss_activity_scores_current "
            f"WHERE ccx_id='{CLASS_A}' AND endsWith(subsection_block_id,'+block@quiz')",
            expected,
        )
    f.dbt("build", "--select", "tag:pss_scores", "--exclude", "resource_type:seed")
    f.dbt(
        "test",
        "--select",
        "mart_measureup_attempt_numbering",
        "rpt_measureup_attempts",
        "rpt_measureup_question_performance",
        "int_pss_learner_class_context",
    )
    verify_historical(f)

    # Reload the header-only production defaults after testing approved fixture rules.
    f.dbt(
        "seed", "--select", "pss_measureup_attempt_rules", "pss_measureup_score_units"
    )
    refresh(f)
    f.expect(
        "an unconfigured provider sitting preserves raw score, version and unknown eligibility",
        "SELECT raw_score, score_status, isNull(score_pct), catalog_cut_score, "
        "version_status, isNull(cut_score), eligibility_status, isNull(is_participation), "
        "isNull(is_qualifying_final), isNull(attempt_number) "
        "FROM reporting.rpt_pss_activity_attempts WHERE exam_attempt_id='102'",
        "80\tscore_unit_unconfirmed\t1\t75\tversion_pairing_unconfirmed\t1\t"
        "eligibility_rule_missing\t1\t1\t1",
    )


def main():
    args = schema_paths(__doc__)
    with database(
        args.tutor_pearson, args.tutor_aspects
    ) as port, tempfile.TemporaryDirectory() as scratch:
        fixture = Fixture(port, Path(scratch))
        provision(fixture, args.tutor_pearson, args.tutor_aspects)
        exercise(fixture)


if __name__ == "__main__":
    main()
