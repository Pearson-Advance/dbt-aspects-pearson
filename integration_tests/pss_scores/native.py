"""Scoring and Quiz-item acceptance cases for the disposable PSS replay."""

import json
from uuid import UUID


from sources import ACTOR, CLASS_A, MASTER


COURSE = MASTER.removeprefix("course-v1:")
STAMP = "2026-11-12 10:30:00"
EARNED = "http://id.tincanapi.com/verb/earned"
EVALUATED = "https://w3id.org/xapi/acrossx/verbs/evaluated"


def statement(number, verb, block_type, block_id, score, succeeded=None):
    result = {"score": score}
    if succeeded is not None:
        result["success"] = succeeded
    key = f"ccx-block-v1:{COURSE}+ccx@101+type@{block_type}+block@{block_id}"
    event_id = str(UUID(int=number))
    event = {
        "id": event_id,
        "timestamp": "2026-11-12T10:30:00Z",
        "verb": {"id": verb},
        "actor": {"account": {"name": ACTOR}},
        "object": {"id": f"https://lms.example/xblock/{key}"},
        "context": {
            "contextActivities": {
                "parent": [
                    {
                        "id": f"https://lms.example/course/{CLASS_A}",
                        "definition": {
                            "type": "http://adlnet.gov/expapi/activities/course"
                        },
                    }
                ]
            }
        },
        "result": result,
    }
    return {"event_id": event_id, "emission_time": STAMP, "event": json.dumps(event)}


def load_native(f):
    # These are two individual evaluations. Only the separate earned statement
    # contains the weighted total calculated by LMS for the whole subsection.
    graded = statement(
        10401, EARNED, "sequential", "quiz", {"raw": 1, "max": 10, "scaled": 0.1}
    )
    half = statement(
        10402, EVALUATED, "problem", "one", {"raw": 1, "max": 2, "scaled": 0.5}, False
    )
    f.insert(
        "xapi.xapi_events_all",
        [
            half,
            half,
            statement(
                10403,
                EVALUATED,
                "problem",
                "two",
                {"raw": 0, "max": 8, "scaled": 0},
                False,
            ),
            graded,
            graded,
            statement(
                10404, EARNED, "sequential", "ten", {"raw": 1, "max": 10, "scaled": 0.1}
            ),
            statement(
                10405, EARNED, "sequential", "zero", {"raw": 0, "max": 8, "scaled": 0}
            ),
            statement(10406, EARNED, "sequential", "missing", {"max": 8, "scaled": 0}),
        ],
    )


def verify_native(f):
    grades = "reporting.rpt_pss_activity_scores_current"
    f.expect(
        "weighted LMS subsection score, observed zero and absent raw grade",
        "SELECT splitByString('+block@', subsection_block_id)[-1] AS section, "
        "score_pct, score_source, score_status FROM "
        + grades
        + f" WHERE ccx_id='{CLASS_A}' AND actor_id='{ACTOR}' "
        "AND section IN ('ten','zero','missing') ORDER BY section",
        "missing\t\\N\t\\N\tmissing_grade_field\nten\t10\tnative\tobserved\nzero\t0\tnative\tobserved",
    )
    f.expect(
        "one LMS observation and one conflicting provider score on the same Quiz",
        "SELECT native_grade_count, provider_attempt_count, score_pct, score_status FROM "
        + grades
        + f" WHERE ccx_id='{CLASS_A}' AND actor_id='{ACTOR}' "
        "AND endsWith(subsection_block_id, '+block@quiz')",
        "1\t1\t\\N\tscore_rule_required",
    )
    f.expect(
        "native evaluations retain points and lack a whole-Quiz sitting",
        "SELECT count(), sum(earned_points), sum(possible_points), "
        "countIf(linkage_status='whole_sitting_key_unavailable' AND current_activity_type='Quiz') "
        "FROM reporting_pss_restricted.int_pss_native_item_evidence "
        f"WHERE ccx_id='{CLASS_A}' AND actor_id='{ACTOR}'",
        "2\t1\t10\t2",
    )


def load_items(f):
    def snapshot(number, **fields):
        return {
            "source_id": str(number),
            "operation": "UPSERT",
            "is_deleted": "False",
            "source_updated_at": STAMP,
            "time_last_dumped": STAMP,
            "dump_id": str(UUID(int=20000)),
            "sink_event_id": str(UUID(int=number)),
            "schema_version": "2",
            **fields,
        }

    f.insert(
        "event_sink.measureup_exam_domain",
        [
            snapshot(8101, external_id="D", exam_test_id="11", name="Fixture domain"),
            snapshot(
                8151, external_id="D", exam_test_id="12", name="Different version"
            ),
        ],
    )
    f.insert(
        "event_sink.measureup_exam_subdomain",
        [
            snapshot(
                8102, external_id="SD", exam_domain_id="8101", name="Fixture subdomain"
            ),
            snapshot(
                8152, external_id="SD", exam_domain_id="8151", name="Different version"
            ),
        ],
    )
    f.insert(
        "event_sink.measureup_question",
        [
            snapshot(
                n,
                external_id=f"Q{n}",
                exam_subdomain_id="8152" if n == 8153 else "8102",
                name=f"Fixture question {n}",
            )
            for n in (8103, 8104, 8105, 8153)
        ],
    )
    f.insert(
        "event_sink.measureup_exam_attempt_domain",
        [
            snapshot(
                8111, exam_domain_id="8101", exam_attempt_id="201", average_score="70"
            ),
        ],
    )
    f.insert(
        "event_sink.measureup_exam_attempt_subdomain",
        [
            snapshot(
                8112,
                exam_subdomain_id="8102",
                attempt_domain_id="8111",
                average_score="70",
            ),
        ],
    )

    def answer(number, item, score, status="answered", parent="8112"):
        return snapshot(
            number,
            question_id=item,
            subdomain_attempt_id=parent,
            status=status,
            score=score,
        )

    first = answer(8121, "8103", "50")
    corrected = dict(
        first,
        score="75.5",
        time_last_dumped="2026-11-13 10:30:00",
        sink_event_id=str(UUID(int=19888)),
    )
    f.insert(
        "event_sink.measureup_exam_question_attempt",
        [
            first,
            first,
            corrected,
            answer(8122, "8104", "", "unanswered"),
            answer(8123, "8105", "0"),
            answer(8124, "8105", "1"),
            answer(8125, "8103", "0", parent="unresolved"),
            answer(8126, "8153", "0"),
        ],
    )


def verify_items(f):
    f.expect(
        "corrected, unscored and disputed provider items each occur once",
        "SELECT provider_item_id, source_result_id, provider_item_score, "
        "earned_points, possible_points, result_status FROM reporting.rpt_pss_quiz_item_results "
        f"WHERE ccx_id='{CLASS_A}' AND actor_id='{ACTOR}' AND exam_attempt_id='201' "
        "ORDER BY provider_item_id",
        "8103\t8121\t75.5\t\\N\t\\N\tprovider_score_observed\n"
        "8104\t8122\t\\N\t\\N\t\\N\tunanswered\n"
        "8105\t\\N\t\\N\t\\N\t\\N\twithin_attempt_selection_unapproved",
    )
    f.expect(
        "unlinked and cross-version provider questions stay in restricted evidence",
        "SELECT linkage_status FROM reporting_pss_restricted.int_pss_measureup_quiz_item_evidence "
        "WHERE source_result_id IN ('8125', '8126') ORDER BY source_result_id",
        "missing_subdomain_attempt\nitem_catalog_conflict",
    )
    f.expect(
        "taxonomy assignments and item results preserve the original whole Quiz sitting",
        "SELECT count(), any(score_pct) FROM reporting.rpt_pss_activity_attempts "
        f"WHERE ccx_id='{CLASS_A}' AND actor_id='{ACTOR}' AND exam_attempt_id='201' AND activity_type='Quiz'",
        "1\t70",
    )
    f.expect(
        "one native problem has two different child tags",
        "SELECT uniqExact(subdomain_tag_id) FROM reporting.int_pss_quiz_item_subdomain_map WHERE taxonomy_id=30 "
        f"AND problem_block_id='block-v1:{COURSE}+type@problem+block@one' AND subdomain_tag_id IN (2,3)",
        "2",
    )
