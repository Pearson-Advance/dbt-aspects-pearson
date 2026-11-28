"""Additional producer events and independent reconciliations for class reporting."""

from datetime import datetime, timedelta
import json

from inputs import ACTOR, OTHER, UNKNOWN, membership
from native import EARNED, statement
from sources import (
    CLASS_A,
    CLASS_B,
    MASTER,
    SECTION_TYPES,
    SNAPSHOT,
    START,
    T1,
    block,
    sink,
)

EMPTY = "ccx-v1:EXAMPLE+DEMO+RUN+ccx@104"
FOURTH = "44444444-4444-4444-8444-444444444444"
VERIFIED = "2026-11-21 00:00:00"
EXPIRES = "2099-01-01 00:00:00"
REVISED = "2026-11-15 10:00:00"


def provider_sitting(f, pk, user, ccx, kind, score, started, updated=T1):
    sink(
        f,
        "measureup_lti_launch_context",
        [
            dict(
                source_id=str(pk),
                user_id=str(user),
                course_id=ccx,
                block_id=block("lti_consumer", kind + "-launch"),
                product_id="P",
            )
        ],
        updated,
    )
    finish = datetime.fromisoformat(started) + timedelta(minutes=5)
    sink(
        f,
        "measureup_exam_test_attempt",
        [
            dict(
                source_id=str(pk),
                lti_launch_event_id=str(pk),
                exam_test_id="11",
                flow_state="finished",
                mode="certification" if kind == "final" else "practice",
                attempt_initiated_datetime=started,
                attempt_finished_datetime=finish.isoformat(sep=" "),
                final_score=str(score),
                is_passed=str(score >= 75),
            )
        ],
        updated,
    )


def coach_a_grant(f, revoked=False):
    sink(
        f,
        "course_licensing_instructor_class",
        [
            dict(
                source_id="2",
                instructor_id="2",
                user_id="72",
                username="coach72",
                institution_id="1",
                institution_ccx_id="11",
                class_id=CLASS_A,
            )
        ],
        REVISED if revoked else T1,
        deleted=revoked,
    )


def extend_course(f):
    # L and K are enrolled in both classes. B has four registered learners.
    membership(f, 45, OTHER, 43, CLASS_B, 12)
    membership(f, 46, FOURTH, 45, CLASS_B, 12)
    sink(f, "course_licensing_institution", [dict(source_id="2", active="True")])
    sink(
        f,
        "course_licensing_license",
        [dict(source_id="8", institution_id="2", status="active")],
    )
    sink(
        f,
        "course_licensing_institution_ccx",
        [
            dict(
                source_id="14",
                institution_id="2",
                license_id="8",
                license_status="active",
                ccx_id=EMPTY,
                ccx_name="Other institution",
                master_course=MASTER,
                deleted="False",
            )
        ],
    )
    sink(
        f,
        "course_licensing_instructor_class",
        [
            dict(
                source_id="3",
                instructor_id="2",
                user_id="72",
                username="coach72",
                institution_id="1",
                institution_ccx_id="12",
                class_id=CLASS_B,
            )
        ],
    )
    f.insert(
        "event_sink.course_overviews",
        [
            dict(
                org="EXAMPLE",
                course_key=ccx,
                display_name=ccx,
                course_data_json="{}",
                course_start="2026-11-02 00:00:00",
                course_end="2026-11-30 23:59:59",
                modified=T1,
                time_last_dumped=T1,
            )
            for ccx in (CLASS_A, CLASS_B, EMPTY)
        ],
    )
    # A has a 1/1 final pass, followed by a failing retake from that same learner.
    provider_sitting(f, 110, 42, CLASS_A, "final", 50, "2026-11-02 13:00:00")
    for pk, user, score in [(111, 42, 40), (112, 44, 90), (113, 43, 50), (114, 45, 45)]:
        provider_sitting(f, pk, user, CLASS_B, "final", score, START)
    # All of A attempted this Quiz; K delivered two distinct whole sittings.
    for pk, started in [(210, "2026-11-02 10:15:00"), (211, "2026-11-02 10:30:00")]:
        provider_sitting(f, pk, 43, CLASS_A, "quiz", 50, started)


def lms_events(f):
    events = []
    for pk, actor, ccx, grade in [
        (37001, ACTOR, CLASS_A, 0.8),
        (37002, OTHER, CLASS_A, 0),
        (37005, UNKNOWN, CLASS_B, None),
    ]:
        row = statement(
            pk,
            EARNED,
            "course",
            "course",
            {
                "min": 0,
                "max": 1,
                **({"scaled": grade, "raw": grade} if grade is not None else {}),
            },
        )
        event = json.loads(row["event"])
        event["object"] = {
            "id": "https://lms.example/course/" + ccx,
            "definition": {"type": "http://adlnet.gov/expapi/activities/course"},
        }
        event["context"]["contextActivities"]["parent"][0]["id"] = (
            "https://lms.example/course/" + ccx
        )
        event["actor"]["account"]["name"] = actor
        row["event"] = json.dumps(event)
        events.append(row)
    for pk in (37003, 37004):
        events.append(
            statement(
                pk,
                "https://w3id.org/xapi/dod-isd/verbs/navigated",
                "vertical",
                "quiz-unit",
                {},
            )
        )
    f.insert("xapi.xapi_events_all", events)


def certify(f):
    for ccx in (CLASS_A, CLASS_B):
        common = dict(
            institution_id="1",
            ccx_id=ccx,
            snapshot_id=SNAPSHOT,
            verified_at=VERIFIED,
            valid_until=EXPIRES,
            evidence_ref="synthetic-source-reconciliation",
        )
        f.insert(
            "reporting.pss_reporting_class_coverage_evidence",
            [dict(**common, final_attempts_complete=1, navigation_complete=1)],
        )
        f.insert(
            "reporting.pss_activity_history_coverage",
            [
                dict(**common, subsection_block_id=block("sequential", key))
                for key in SECTION_TYPES
            ],
        )
        f.insert(
            "reporting.pss_attempt_coverage_evidence",
            [
                dict(
                    ccx_id=ccx,
                    subsection_block_id=block("sequential", key),
                    snapshot_id=SNAPSHOT,
                    verified_at=VERIFIED,
                    coverage_start=START,
                    coverage_end="2026-11-02 18:00:00",
                    evidence_ref="synthetic-source-reconciliation",
                )
                for key in SECTION_TYPES
            ],
        )
