"""Exercise the activity inventory against a disposable Ulmo ClickHouse with real producer tables."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from uuid import UUID

import yaml

from disposable import database, schema_paths


ROOT = Path(__file__).resolve().parents[2]
MASTER = "course-v1:EXAMPLE+DEMO+RUN"
CLASS_A = "ccx-v1:EXAMPLE+DEMO+RUN+ccx@101"
CLASS_B = "ccx-v1:EXAMPLE+DEMO+RUN+ccx@102"
T1 = "2026-11-01 10:00:00"
T2 = "2026-11-02 10:00:00"
T3 = "2026-11-03 10:00:00"


def block(kind, key):
    return f"block-v1:EXAMPLE+DEMO+RUN+type@{kind}+block@{key}"


class Fixture:
    def __init__(self, port, scratch):
        self.url = f"http://127.0.0.1:{port}/"
        self.scratch = scratch
        profile = yaml.safe_load((ROOT / ".github/profiles.yml").read_text())
        profile["aspects"]["outputs"]["ci"].update(host="127.0.0.1", port=port)
        (scratch / "profiles.yml").write_text(yaml.safe_dump(profile))

    def sql(self, statement):
        with urlopen(self.url, data=statement.encode(), timeout=60) as response:
            return response.read().decode().strip()

    def insert(self, table, rows):
        if not rows:
            return
        columns = ", ".join(rows[0])
        params = urlencode(
            {
                "query": f"INSERT INTO {table} ({columns}) FORMAT JSONEachRow",
                "input_format_json_named_tuples_as_objects": 1,
            }
        )
        payload = "\n".join(json.dumps(row) for row in rows).encode()
        with urlopen(Request(self.url + "?" + params, data=payload), timeout=60):
            pass

    def dbt(self, command, *flags, configured=False):
        variables = (
            {"pss_quiz_taxonomies_by_master": {MASTER: [30]}} if configured else {}
        )
        subprocess.run(
            [
                "dbt",
                command,
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
                "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            },
        )

    def expect(self, label, sql, expected):
        result = self.sql(sql)
        if result != expected:
            raise AssertionError(f"{label}: expected {expected!r}, got {result!r}")
        print("PASS", label, flush=True)


def lms_inputs(f):
    common = dict(
        operation="UPSERT",
        is_deleted="False",
        source_updated_at=T1,
        time_last_dumped=T1,
    )
    f.insert(
        "event_sink.course_licensing_course",
        [dict(source_id="9", master_course_id=MASTER, **common)],
    )
    f.insert(
        "event_sink.course_licensing_institution",
        [dict(source_id="1", active="True", **common)],
    )
    f.insert(
        "event_sink.course_licensing_license",
        [dict(source_id="7", institution_id="1", status="active", **common)],
    )
    classes = [
        dict(
            source_id=str(n),
            institution_id="1",
            license_id="7",
            license_status="active",
            ccx_id=key,
            ccx_name=f"Class {n}",
            master_course=MASTER,
            deleted=deleted,
            **common,
        )
        for n, key, deleted in [
            (11, CLASS_A, "False"),
            (12, CLASS_B, "False"),
            (13, "ccx-v1:EXAMPLE+DEMO+RUN+ccx@103", "True"),
        ]
    ]
    f.insert("event_sink.course_licensing_institution_ccx", classes)
    f.insert(
        "event_sink.course_overviews",
        [
            dict(
                org="EXAMPLE",
                course_key=MASTER,
                display_name="Demo",
                course_data_json="{}",
                modified=T1,
                time_last_dumped=T1,
            ),
        ],
    )
    # A repeated child tag, another subdomain, its parent, one domain-only
    # problem and one untagged problem. No attempt or learner input is needed.
    f.insert(
        "event_sink.taxonomy",
        [dict(id=30, name="Fixture objectives", time_last_dumped=T1)],
    )
    tags = [(1, 0, "Domain"), (2, 1, "X"), (3, 1, "Y")]
    f.insert(
        "event_sink.tag",
        [
            dict(
                id=i,
                taxonomy=30,
                parent=parent,
                value=value,
                external_id=f"E{i}",
                lineage=json.dumps(["Domain", value] if parent else [value]),
                time_last_dumped=T1,
            )
            for i, parent, value in tags
        ],
    )
    f.insert(
        "event_sink.object_tag",
        [
            dict(
                id=i,
                object_id=block("problem", problem),
                taxonomy=30,
                tag=tag,
                time_last_dumped=T1,
            )
            for i, (problem, tag) in enumerate(
                [("one", 1), ("one", 2), ("one", 2), ("one", 3), ("two", 1)], 1
            )
        ],
    )


def publish(
    f,
    revision,
    timestamp,
    quiz_type,
    with_lab,
    *,
    replay_dump=None,
    source_only=False,
    quiz_problems=3,
):
    dump_time = replay_dump or timestamp
    snapshot = f"{revision:024x}"

    def subsection(name, grading, seq, problems):
        return dict(
            subsection_block_id=block("sequential", name),
            assignment_type=grading,
            graded=1,
            subsection_name=name,
            section_block_id=block("chapter", "section"),
            section_name="section",
            section_order=2,
            subsection_order=seq,
            problem_count=problems,
            lti_consumer_count=0,
            skillable_xblock_count=int(name == "lab"),
            other_component_count=0,
        )

    rows = [subsection("quiz", quiz_type, 3, quiz_problems)]
    if with_lab:
        rows.append(subsection("lab", "Simulation", 8, 0))
    f.insert(
        "event_sink.pss_subsection_assignment_types",
        [
            dict(
                course_key=MASTER,
                published_at=timestamp,
                time_last_dumped=dump_time,
                snapshot_id=snapshot,
                head_confirmed_at=None if replay_dump else dump_time,
                subsections=rows,
            ),
        ],
    )
    if replay_dump or source_only:
        return
    structure = [
        ("course", "course", 0, 0, 0, 0),
        ("chapter", "section", 1, 0, 0, 0),
        ("sequential", "quiz", 1, 1, 0, 1),
        ("vertical", "unit", 1, 1, 1, 1),
    ]
    structure.extend(("problem", key, 1, 1, 1, 1) for key in ["one", "two", "three"])
    if with_lab:
        structure.extend(
            [
                ("sequential", "lab", 1, 2, 0, 1),
                ("vertical", "labunit", 1, 2, 1, 1),
                ("skillable_xblock", "labtask", 1, 2, 1, 1),
            ]
        )
    f.insert(
        "event_sink.course_blocks",
        [
            dict(
                org="EXAMPLE",
                course_key=MASTER,
                location=block(kind, key),
                display_name=key,
                xblock_data_json=json.dumps(
                    dict(
                        section=section, subsection=sub, unit=unit, graded=bool(graded)
                    )
                ),
                order=i,
                edited_on=timestamp,
                dump_id=str(UUID(int=revision)),
                time_last_dumped=timestamp,
            )
            for i, (kind, key, section, sub, unit, graded) in enumerate(structure, 1)
        ],
    )


def refresh_structure(f):
    f.sql("SYSTEM REFRESH VIEW reporting.dim_course_blocks_mv")
    f.sql("SYSTEM WAIT VIEW reporting.dim_course_blocks_mv")


def configure(f):
    f.insert(
        "reporting.pss_assignment_type_mapping",
        [
            dict(master_course=MASTER, assignment_type=name, activity_type=activity)
            for name, activity in [("Knowledge Check", "Quiz"), ("Simulation", "Lab")]
        ],
    )
    f.insert(
        "reporting.pss_curriculum_applicability",
        [
            dict(
                master_course=MASTER,
                ccx_id="",
                snapshot_id="",
                rule="inherit_master",
                subsection_block_id="",
            )
        ],
    )
    f.insert(
        "reporting.pss_grading_policy",
        [
            dict(
                master_course=MASTER,
                activity_type=kind,
                weight=weight,
                final_exam_pass_rule=(
                    "native_pass_mark" if kind == "Final Exam" else None
                ),
                final_exam_pass_mark=75 if kind == "Final Exam" else None,
            )
            for kind, weight in [
                ("Quiz", 20),
                ("Lab", 0),
                ("Practice Exam", 50),
                ("Final Exam", 30),
            ]
        ],
    )


def exercise(f):
    # Only a fresh, local, purpose-built server is a fixture target. Never
    # overwrite existing reporting models or seed data on a development stack.
    f.expect(
        "fresh reporting database",
        "SELECT count() FROM system.tables WHERE database='reporting'",
        "0",
    )
    for table in [
        "course_licensing_institution_ccx",
        "course_overviews",
        "course_blocks",
        "pss_subsection_assignment_types",
        "object_tag",
        "tag",
        "taxonomy",
        "course_licensing_course",
        "course_licensing_license",
        "course_licensing_institution",
    ]:
        f.expect(
            f"empty fixture input {table}",
            f"SELECT count() FROM event_sink.{table}",
            "0",
        )
    lms_inputs(f)
    publish(f, 1, T1, "Knowledge Check", True)
    f.dbt("seed", "--select", "tag:pss_insights")
    f.dbt("run", "--select", "+tag:pss_insights")
    selected = [
        "--select",
        "tag:pss_insights",
        "int_tags_object_tag_current",
        "--exclude",
        "resource_type:seed",
    ]
    f.dbt("build", *selected)
    catalog = "reporting.int_pss_activity_catalog"
    master_coverage = (
        "SELECT inventory_status FROM reporting.int_pss_master_inventory_coverage"
    )
    policy = "SELECT policy_status FROM reporting.int_pss_grading_policy_coverage"
    item_coverage = " FROM reporting.int_pss_quiz_item_subdomain_coverage"
    f.expect(
        "default class population",
        "SELECT count() FROM reporting.int_pss_class_scope",
        "2",
    )
    f.expect("no implicit assignment mapping", master_coverage, "graded_not_classified")
    f.expect("no implicit curriculum approval", f"SELECT count() FROM {catalog}", "0")
    f.expect("policy missing", policy, "missing")
    f.expect(
        "unconfigured taxonomy", "SELECT configured_taxonomy_count" + item_coverage, "0"
    )

    f.expect(
        "unknown Quiz population without a type mapping",
        "SELECT quiz_structure_status, isNull(quiz_problem_count)" + item_coverage,
        "classification_incomplete\t1",
    )

    configure(f)
    f.dbt("build", *selected, configured=True)
    f.expect(
        "two eligible classes with two ordered activities",
        f"SELECT ccx_id, groupArray(activity_type) FROM "
        f"(SELECT * FROM {catalog} WHERE in_latest_publish ORDER BY ccx_id, activity_sequence) "
        "GROUP BY ccx_id ORDER BY ccx_id",
        f"{CLASS_A}\t['Quiz','Lab']\n{CLASS_B}\t['Quiz','Lab']",
    )
    f.expect(
        "class-scoped learning-event key",
        f"SELECT ccx_subsection_block_id FROM {catalog} WHERE ccx_id='{CLASS_B}' AND activity_type='Quiz'",
        "ccx-block-v1:EXAMPLE+DEMO+RUN+ccx@102+type@sequential+block@quiz",
    )
    f.expect(
        "zero weight and native final rule",
        "SELECT countIf(weight=0 AND activity_type='Lab'), "
        "countIf(final_exam_pass_mark=75 AND final_exam_pass_rule='native_pass_mark') "
        "FROM reporting.int_pss_grading_policy",
        "1\t1",
    )
    f.expect(
        "two child assignments on one Quiz problem, a domain-only problem and an unmapped one",
        "SELECT quiz_problem_count, quiz_problems_with_subdomain, quiz_problems_domain_only, "
        "quiz_problems_without_mapping" + item_coverage,
        "3\t1\t1\t1",
    )

    # Studio has published a fourth problem. The Aspects structure refresh
    # still has three, even though both feeds agree on the subsection keys.
    publish(
        f,
        5,
        "2026-11-01 13:00:00",
        "Knowledge Check",
        True,
        source_only=True,
        quiz_problems=4,
    )
    f.expect(
        "Quiz totals await a structure refresh",
        "SELECT quiz_structure_status, isNull(quiz_problem_count), "
        "isNull(quiz_problems_with_subdomain)" + item_coverage,
        "structure_disagrees\t1\t1",
    )
    f.expect(
        "stale Quiz mappings withheld",
        "SELECT count() FROM reporting.int_pss_quiz_item_subdomain_map",
        "0",
    )

    # The same checked-in mapping now classifies the former Quiz as Lab.
    publish(f, 2, T2, "Simulation", False)
    refresh_structure(f)
    f.expect(
        "one reclassified current activity and one historical Lab per class",
        f"SELECT countIf(in_latest_publish AND activity_type='Lab' AND activity_sequence=1), "
        f"countIf(NOT in_latest_publish AND activity_type='Lab' AND isNull(activity_sequence)) FROM {catalog}",
        "2\t2",
    )
    publish(f, 3, T3, "Homework", False)
    publish(f, 1, T1, "Knowledge Check", True, replay_dump="2026-11-04 12:00:00")
    refresh_structure(f)
    f.expect(
        "new unmapped type survives late replay",
        master_coverage,
        "graded_not_classified",
    )
    f.expect(
        "current catalog excludes Homework, removed Lab remains linkable",
        f"SELECT countIf(in_latest_publish), countIf(NOT in_latest_publish) FROM {catalog}",
        "0\t2",
    )


def verify_newer_and_ungraded(f):
    # A publish can change only Graded as. A delivery of an older revision
    # after that publish has no head confirmation and cannot validate the new type.
    latest = "2026-11-05 11:00:00"
    f.insert(
        "event_sink.course_overviews",
        [
            dict(
                org="EXAMPLE",
                course_key=MASTER,
                display_name="Demo",
                course_data_json="{}",
                modified=latest,
                time_last_dumped=latest,
            )
        ],
    )
    publish(f, 3, T3, "Homework", False, replay_dump="2026-11-06 10:00:00")
    status = "SELECT inventory_status FROM reporting.int_pss_master_inventory_coverage"
    f.expect(
        "same-structure publication still awaits assignment evidence",
        status,
        "newer_course_overview",
    )
    f.insert(
        "event_sink.pss_subsection_assignment_types",
        [
            dict(
                course_key=MASTER,
                published_at=latest,
                time_last_dumped=latest,
                snapshot_id=f"{4:024x}",
                head_confirmed_at=latest,
                subsections=[
                    dict(
                        subsection_block_id=block("sequential", "reading"),
                        assignment_type="",
                        graded=0,
                        subsection_name="reading",
                        section_block_id=block("chapter", "section"),
                        section_name="section",
                        section_order=2,
                        subsection_order=3,
                        problem_count=0,
                        lti_consumer_count=0,
                        skillable_xblock_count=0,
                        other_component_count=0,
                    )
                ],
            )
        ],
    )
    f.insert(
        "event_sink.course_blocks",
        [
            dict(
                org="EXAMPLE",
                course_key=MASTER,
                location=block(kind, key),
                display_name=key,
                xblock_data_json=json.dumps(
                    dict(section=1, subsection=sub, unit=0, graded=False)
                ),
                order=i,
                edited_on=latest,
                dump_id=str(UUID(int=4)),
                time_last_dumped=latest,
            )
            for i, (kind, key, sub) in enumerate(
                [("chapter", "section", 0), ("sequential", "reading", 1)], 2
            )
        ],
    )
    refresh_structure(f)
    f.expect("complete ungraded publication", status, "no_graded_subsections")
    f.expect(
        "ungraded reading excluded from coverage",
        "SELECT countIf(in_latest_publish) FROM reporting.int_pss_activity_type_coverage",
        "0",
    )


def main():
    args = schema_paths(__doc__)
    with database(
        args.tutor_pearson, args.tutor_aspects
    ) as port, tempfile.TemporaryDirectory() as directory:
        fixture = Fixture(port, Path(directory))
        exercise(fixture)
        verify_newer_and_ungraded(fixture)


if __name__ == "__main__":
    main()
