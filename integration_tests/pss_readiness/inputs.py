"""Synthetic course, policy, whole sittings and provider item trees for readiness."""

from datetime import datetime, timedelta

from sources import (
    ACTOR,
    CLASS_A,
    CLASS_B,
    MASTER,
    SNAPSHOT,
    T1,
    block,
    configure,
    load_scope,
    sink,
)


OTHER = "22222222-2222-4222-8222-222222222222"
UNKNOWN = "33333333-3333-4333-8333-333333333333"
START = "2026-11-02 10:00:00"
END = "2026-11-02 11:00:00"
NEXT = "2026-11-02 12:00:00"
TYPES = ["Quiz", "Lab", "Practice Exam"]


def membership(f, pk, actor, user, ccx, class_pk):
    f.insert(
        "event_sink.external_id",
        [
            dict(
                user_id=user,
                external_user_id=actor,
                external_id_type="xapi",
                time_last_dumped=T1,
            )
        ],
    )
    sink(
        f,
        "course_licensing_licensed_enrollment",
        [
            dict(
                source_id=str(pk),
                id=str(pk),
                institution_id="1",
                institution_ccx_id=str(class_pk),
                license_id="7",
                user_id=str(user),
                student="fixture-learner",
                class_id=ccx,
                is_active="True",
                expired="False",
            )
        ],
    )


def sitting(f, pk, kind, score, user=42, started=START, mode="practice"):
    launch = dict(
        source_id=str(pk),
        user_id=str(user),
        course_id=CLASS_A,
        block_id=block("lti_consumer", kind + "-launch"),
        product_id="P",
    )
    finished = datetime.fromisoformat(started) + timedelta(minutes=5)
    row = dict(
        source_id=str(pk),
        lti_launch_event_id=str(pk),
        exam_test_id="11",
        flow_state="finished",
        mode=mode,
        attempt_initiated_datetime=started,
        attempt_finished_datetime=finished.isoformat(sep=" "),
        final_score=str(score),
        is_passed="True",
    )
    sink(f, "measureup_lti_launch_context", [launch])
    sink(f, "measureup_exam_test_attempt", [row])


def practice_items(f, attempt_id, groups):
    """Persist one distinct Django question per position in its domain and version."""
    dom_attempt = str(attempt_id * 10)
    sink(
        f,
        "measureup_exam_attempt_domain",
        [
            dict(
                source_id=dom_attempt,
                exam_domain_id="500",
                exam_attempt_id=str(attempt_id),
            )
        ],
    )
    sub_attempts, questions, answers = [], [], []
    for sub, correct, total in groups:
        sub_id = 510 + sub
        sub_attempt = str(attempt_id * 100 + sub)
        sub_attempts.append(
            dict(
                source_id=sub_attempt,
                exam_subdomain_id=str(sub_id),
                attempt_domain_id=dom_attempt,
            )
        )
        for position in range(total):
            item = str(sub_id * 100 + position)
            questions.append(
                dict(
                    source_id=item,
                    external_id=f"Q{item}",
                    name=f"Q{item}",
                    exam_subdomain_id=str(sub_id),
                )
            )
            answers.append(
                dict(
                    source_id=str(attempt_id * 10000 + sub * 100 + position),
                    question_id=item,
                    subdomain_attempt_id=sub_attempt,
                    status="answered",
                    score="1" if position < correct else "0",
                )
            )
    sink(f, "measureup_exam_attempt_subdomain", sub_attempts)
    sink(f, "measureup_question", questions)
    sink(f, "measureup_exam_question_attempt", answers + [answers[0]])


def load_source_rows(f):
    load_scope(f, lab_lti=True)
    membership(f, 43, OTHER, 43, CLASS_A, 11)
    membership(f, 44, UNKNOWN, 44, CLASS_B, 12)
    sink(
        f,
        "measureup_exam_test",
        [
            dict(
                source_id="11",
                product_id="P",
                name="Fixture test",
                cut_score="75",
                exam_last_updated="2026-10-10 12:00:00",
            )
        ],
    )
    sink(
        f,
        "measureup_exam_domain",
        [dict(source_id="500", external_id="core", exam_test_id="11", name="Core")],
    )
    sink(
        f,
        "measureup_exam_subdomain",
        [
            dict(
                source_id=str(510 + i),
                external_id=name,
                exam_domain_id="500",
                name=name,
            )
            for i, name in enumerate("ABCDE")
        ],
    )
    sitting(f, 201, "quiz", 60)
    sitting(f, 301, "practice", 50, started=END)
    sitting(f, 302, "practice", 80, started=NEXT)
    # These two observations must never enter the PSS readiness numerator.
    sitting(f, 101, "final", 100, mode="certification")
    sitting(f, 303, "practice", 100, mode="certification")
    practice_items(f, 301, [(0, 1, 2)])
    practice_items(f, 302, [(0, 8, 10)])
    sitting(f, 401, "practice", 25, user=43, started=END)
    sitting(f, 402, "practice", 0, user=43, started=NEXT)
    practice_items(f, 401, [(0, 1, 5), (1, 2, 5), (2, 2, 5), (3, 3, 5), (4, 7, 10)])
    practice_items(f, 402, [(0, 0, 5)])
    sink(
        f,
        "measureup_exam_ready_status",
        [
            dict(
                source_id="900",
                course_enrollment_id="41",
                user_id="42",
                course_id=CLASS_A,
                enrollment_mode="verified",
                enrollment_is_active="True",
                status="fixture-provider-status",
                last_score="0.97",
                status_updated_at=T1,
            )
        ],
    )


def configure_readiness(f):
    configure(f)
    f.insert(
        "reporting.pss_grading_policy",
        [
            dict(
                master_course=MASTER,
                activity_type=t,
                weight=w,
                final_exam_pass_rule=(
                    "measureup_cut_score" if t == "Final Exam" else None
                ),
                final_exam_pass_mark=None,
            )
            for t, w in zip([*TYPES, "Final Exam"], [20, 30, 40, 10])
        ],
    )
    f.insert(
        "reporting.pss_measureup_attempt_rules",
        [
            dict(
                master_course=MASTER,
                flow_state="finished",
                activity_type=t,
                mode=m,
                is_participation=1,
                is_eligible_practice=0,
                is_qualifying_final=0,
            )
            for t, m in [("Practice Exam", "certification"), ("Lab", "practice")]
        ],
    )
    f.insert(
        "reporting.pss_readiness_history_coverage",
        [
            dict(
                institution_id="1",
                ccx_id=ccx,
                actor_id=actor,
                snapshot_id=SNAPSHOT,
                activity_type=t,
                coverage_start="",
                coverage_end="",
                verified_at="2026-11-21 00:00:00",
                valid_until="2099-01-01 00:00:00",
                evidence_ref="synthetic-reconciliation",
            )
            for ccx, actor in [(CLASS_A, ACTOR), (CLASS_B, ACTOR), (CLASS_A, OTHER)]
            for t in TYPES
        ],
    )
    f.insert(
        "reporting.pss_attempt_coverage_evidence",
        [
            dict(
                ccx_id=CLASS_A,
                subsection_block_id=block("sequential", "practice"),
                snapshot_id=SNAPSHOT,
                coverage_start=START,
                coverage_end=NEXT,
                verified_at="2026-11-03 12:00:00",
                evidence_ref="fixture-practice-ledger",
            )
        ],
    )
