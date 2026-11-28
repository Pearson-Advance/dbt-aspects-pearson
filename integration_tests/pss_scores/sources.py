"""Load the additional Ulmo deployment DDL and synthetic assessment records."""

import json
import re
from uuid import UUID

from disposable import table_statement
from replay import CLASS_A, CLASS_B, MASTER, T1, block, lms_inputs


ACTOR = "11111111-1111-4111-8111-111111111111"
SNAPSHOT = f"{1:024x}"
START = "2026-11-02 10:00:00"
LATER = "2026-11-02 11:00:00"
END = "2026-11-02 11:30:00"
CORRECTION = "2026-11-03 09:00:00"
REMOVAL = "2026-11-04 09:00:00"
SECTION_TYPES = {
    "quiz": "Quiz",
    "lab": "Lab",
    "practice": "Practice Exam",
    "final": "Final Exam",
    "ten": "Quiz",
    "zero": "Quiz",
    "missing": "Quiz",
}
PROBLEMS = {
    "quiz": ["one", "two", "three"],
    "ten": ["ten-problem"],
    "zero": ["zero-problem"],
    "missing": ["missing-problem"],
}


def provision(f, pearson, aspects):
    script = (pearson / "tutor_pearson_plugin/services/aspects/defaults.py").read_text()
    for name in re.findall(
        r"CREATE TABLE IF NOT EXISTS event_sink\.((?:measureup|course_licensing)_\w+)",
        script,
    ):
        f.sql(table_statement(script, name))
    migrations = (
        aspects
        / "tutoraspects/templates/aspects/apps/aspects/migrations/alembic/versions"
    )
    for prefix, table, engine in [
        ("0026", "event_sink.external_id", "ReplacingMergeTree"),
        ("0027", "event_sink.user_profile", "ReplacingMergeTree"),
        ("0032", "xapi.xapi_events_all", "ReplacingMergeTree"),
    ]:
        source = next(migrations.glob(prefix + "_*.py")).read_text()
        source = source.replace("{{ ASPECTS_XAPI_DATABASE }}", "xapi").replace(
            "{{ ASPECTS_RAW_XAPI_TABLE }}", "xapi_events_all"
        )
        source = (
            source.replace("{{ ASPECTS_EVENT_SINK_DATABASE }}", "event_sink")
            .replace("{on_cluster}", "")
            .replace("{engine}", engine)
        )
        statement = re.search(
            r"CREATE (?:TABLE IF NOT EXISTS|OR REPLACE TABLE) "
            + re.escape(table)
            + r'\s*\(.*?(?:"""|;)',
            source,
            re.S,
        ).group()
        f.sql("CREATE DATABASE IF NOT EXISTS " + table.split(".")[0])
        f.sql(statement.removesuffix('"""').strip())
    # Follow the later schema migrations for the fields consumed by the class context.
    for prefix in ["0031", "0040"]:
        source = next(migrations.glob(prefix + "_*.py")).read_text()
        alter = re.search(
            r'ALTER TABLE .*?ADD COLUMN .*?(?:"""|;)', source, re.S
        ).group()
        alter = (
            alter.removesuffix('"""')
            .replace("{{ ASPECTS_EVENT_SINK_DATABASE }}", "event_sink")
            .replace("{on_cluster}", "")
        )
        f.sql(alter)
    for prefix in ["0020", "0033"]:
        source = next(migrations.glob(prefix + "_*.py")).read_text()
        f.sql(re.search(r"CREATE OR REPLACE FUNCTION .*?;", source, re.S).group())


def sink(f, name, data, timestamp=T1, deleted=False):
    f.insert(
        "event_sink." + name,
        [
            dict(
                **record,
                operation="DELETE" if deleted else "UPSERT",
                is_deleted="True" if deleted else "False",
                source_updated_at=timestamp,
                time_last_dumped=timestamp,
            )
            for record in data
        ],
    )


def publication(f, *, revision=1, published_at=T1, quiz_assignment="Quiz", lab_lti=False):
    subsections, structure = [], [
        ("course", "course", 0, 0, 0),
        ("chapter", "section", 1, 0, 0),
    ]
    for pos, (key, activity_type) in enumerate(SECTION_TYPES.items(), 1):
        problems = PROBLEMS.get(key, [])
        lti = key in ("final", "quiz", "practice") or (key == "lab" and lab_lti)
        structure.extend(
            [("sequential", key, 1, pos, 0), ("vertical", key + "-unit", 1, pos, 1)]
        )
        children = [("problem", p) for p in problems]
        if lti:
            children.append(("lti_consumer", key + "-launch"))
        if key == "lab":
            children.append(("skillable_xblock", "labtask"))
        structure.extend((kind, child, 1, pos, 1) for kind, child in children)
        subsections.append(
            dict(
                subsection_block_id=block("sequential", key),
                assignment_type=quiz_assignment if key == "quiz" else activity_type,
                graded=1,
                subsection_name=key,
                section_block_id=block("chapter", "section"),
                section_name="section",
                section_order=1,
                subsection_order=pos,
                problem_count=len(problems),
                lti_consumer_count=int(lti),
                skillable_xblock_count=int(key == "lab"),
                other_component_count=0,
            )
        )
    f.insert(
        "event_sink.pss_subsection_assignment_types",
        [
            dict(
                course_key=MASTER,
                published_at=published_at,
                time_last_dumped=published_at,
                snapshot_id=f"{revision:024x}",
                head_confirmed_at=published_at,
                subsections=subsections,
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
                    dict(section=section, subsection=sub, unit=unit, graded=sub > 0)
                ),
                order=order,
                edited_on=published_at,
                dump_id=str(UUID(int=revision)),
                time_last_dumped=published_at,
            )
            for order, (kind, key, section, sub, unit) in enumerate(structure, 1)
        ],
    )


def load_scope(f, *, lab_lti=False):
    lms_inputs(f)
    publication(f, lab_lti=lab_lti)
    f.insert(
        "event_sink.external_id",
        [
            dict(
                external_user_id=ACTOR,
                external_id_type="xapi",
                user_id=42,
                time_last_dumped=T1,
            )
        ],
    )
    enrollments = [
        dict(
            source_id=str(idx),
            id=str(idx),
            institution_id="1",
            institution_ccx_id=str(class_pk),
            license_id="7",
            user_id="42",
            student="fixture-learner",
            class_id=ccx,
            is_active="True",
            expired="False",
        )
        for idx, class_pk, ccx in [(41, 11, CLASS_A), (42, 12, CLASS_B)]
    ]
    sink(f, "course_licensing_licensed_enrollment", enrollments + [enrollments[0]])
    sink(
        f,
        "course_licensing_course_enrollment_allowed",
        [
            dict(
                source_id="90",
                id="90",
                email="invited@example.invalid",
                course_id=CLASS_A,
            )
        ],
    )
    for i in (1, 2):
        user = str(70 + i)
        f.insert(
            "event_sink.user_profile",
            [
                dict(
                    id=i,
                    user_id=int(user),
                    username="coach" + user,
                    time_last_dumped=T1,
                )
            ],
        )
        sink(
            f,
            "course_licensing_instructor",
            [
                dict(
                    source_id=str(i),
                    user_id=user,
                    username="coach" + user,
                    user_is_active="True",
                )
            ],
        )
        sink(
            f,
            "course_licensing_instructor_institution",
            [
                dict(
                    source_id=str(i),
                    instructor_id=str(i),
                    user_id=user,
                    username="coach" + user,
                    institution_id="1",
                    active="True",
                )
            ],
        )
        sink(
            f,
            "course_licensing_instructor_class",
            [
                dict(
                    source_id=str(i),
                    instructor_id=str(i),
                    user_id=user,
                    username="coach" + user,
                    institution_id="1",
                    institution_ccx_id="11",
                    class_id=CLASS_A,
                )
            ],
        )


def configure(f):
    f.insert(
        "reporting.pss_assignment_type_mapping",
        [
            dict(master_course=MASTER, assignment_type=t, activity_type=t)
            for t in set(SECTION_TYPES.values())
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
        "reporting.pss_measureup_score_units",
        [dict(master_course=MASTER, score_unit="percent", accept_version_pairing=1)],
    )
    f.insert(
        "reporting.pss_measureup_attempt_rules",
        [
            dict(
                master_course=MASTER,
                activity_type=kind,
                mode=mode,
                flow_state="finished",
                is_participation=1,
                is_eligible_practice=int(
                    kind == "Practice Exam" and mode == "practice"
                ),
                is_qualifying_final=int(
                    kind == "Final Exam" and mode == "certification"
                ),
            )
            for kind, mode in [
                ("Final Exam", "certification"),
                ("Final Exam", "practice"),
                ("Practice Exam", "practice"),
                ("Quiz", "practice"),
            ]
        ],
    )


def attempt(pk, version, score, mode="certification", started=START):
    return dict(
        source_id=str(pk),
        lti_launch_event_id=str(pk),
        exam_test_id=str(version),
        flow_state="finished",
        mode=mode,
        attempt_initiated_datetime=started,
        attempt_finished_datetime=END,
        final_score=score,
        is_passed="True" if pk != 101 else "False",
    )


def load_attempts(f):
    for idx, mark in [(11, 75), (12, 85)]:
        sink(
            f,
            "measureup_exam_test",
            [
                dict(
                    source_id=str(idx),
                    product_id="P",
                    name="Versioned test",
                    cut_score=str(mark),
                    exam_last_updated=f"2026-10-{idx:02d} 00:00:00",
                )
            ],
        )
    configs = [
        (101, 11, "60", "final", "certification", START),
        (102, 11, "72", "final", "certification", START),
        (103, 12, "80", "final", "certification", LATER),
        (104, 99, "80", "final", "certification", END),
        (105, 11, None, "final", "certification", END),
        (106, 11, "65", "final", "practice", END),
        (201, 11, "70", "quiz", "practice", START),
        (301, 11, "85", "practice", "practice", START),
        (401, 11, "55", "missing-launch", "practice", START),
        (402, 11, "45", "unmapped", "practice", START),
        (403, 11, "55", "quiz", "practice", START),
    ]
    for pk, ver, score, key, mode, started in configs:
        if key != "missing-launch":
            usage = block("lti_consumer", key + "-launch")
            if pk % 2 == 0:
                usage = usage.replace("block-v1:", "ccx-block-v1:").replace(
                    "+type@", "+ccx@101+type@"
                )
            sink(
                f,
                "measureup_lti_launch_context",
                [
                    dict(
                        source_id=str(pk),
                        user_id="999" if pk == 403 else "42",
                        course_id=CLASS_A,
                        block_id=usage,
                        product_id="P",
                    )
                ],
            )
        sink(f, "measureup_exam_test_attempt", [attempt(pk, ver, score, mode, started)])
    sink(f, "measureup_exam_test_attempt", [attempt(102, 11, "72")])
    sink(
        f,
        "measureup_lti_launch_context",
        [
            dict(
                source_id="408",
                user_id="42",
                course_id=CLASS_A,
                block_id=block("lti_consumer", "quiz-launch"),
                product_id="P",
            )
        ],
    )


def verify_historical(f):
    """An activity losing its current mapping still identifies its earlier sittings."""
    publication(
        f, revision=2, published_at="2026-11-15 10:00:00", quiz_assignment="Homework"
    )
    for view in (
        "reporting.dim_course_blocks_mv",
        "reporting.rpt_pss_activity_attempts_mv",
    ):
        f.sql("SYSTEM REFRESH VIEW " + view)
        f.sql("SYSTEM WAIT VIEW " + view)
    f.expect(
        "republished unmapped Quiz is absent from the current catalog",
        "SELECT count() FROM reporting.int_pss_activity_catalog "
        f"WHERE ccx_id='{CLASS_A}' AND subsection_block_id='{block('sequential','quiz')}'",
        "0",
    )
    f.expect(
        "Quiz sitting retains its observed historical assignment",
        "SELECT activity_type, attempt_catalog_snapshot_id, classification_status FROM "
        "reporting.rpt_pss_activity_attempts WHERE exam_attempt_id='201'",
        "Quiz\t" + SNAPSHOT + "\tclassified",
    )
