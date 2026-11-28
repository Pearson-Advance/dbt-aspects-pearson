"""Attested native Quiz sittings and independent MeasureUp binary scores."""

import json
from uuid import UUID

from inputs import practice_items
from native import EVALUATED, statement
from sources import ACTOR, CLASS_A, MASTER, SNAPSHOT, block, sink


CONTEXT = "event_sink.fixture_native_quiz_context"
REPORT = "reporting.rpt_pss_quiz_subdomain_results"
SCOPE = f"ccx_id='{CLASS_A}' AND actor_id='{ACTOR}'"
DOMAIN = (30, 1, 0)
X = (30, 1, 2)
Y = (30, 1, 3)


def context(event_id, attempt, problem, objectives=None):
    return dict(
        ccx_id=CLASS_A,
        actor_id=ACTOR,
        source_result_id=str(UUID(int=event_id)),
        attempt_key="native:" + attempt,
        attempted_at="2026-11-12 10:00:00",
        subsection_block_id=block("sequential", "quiz"),
        problem_block_id=block("problem", problem),
        snapshot_id=SNAPSHOT,
        evidence_ref="fixture-selected-result-and-eligible-sitting",
        mapping_ref="fixture-versioned-assignments" if objectives is not None else "",
        objectives=objectives or [],
    )


def prepare_quiz(f):
    f.sql(
        "CREATE TABLE " + CONTEXT + " (ccx_id String, actor_id String, "
        "source_result_id String, attempt_key String, attempted_at DateTime64(6,'UTC'), "
        "subsection_block_id String, problem_block_id String, snapshot_id String, "
        "evidence_ref String, mapping_ref String, objectives Array(Tuple(UInt64,UInt64,UInt64))) "
        "ENGINE=MergeTree ORDER BY (ccx_id, actor_id, source_result_id)"
    )

    split = context(33001, "t5", "one", [DOMAIN, X, X, Y])
    f.insert(
        CONTEXT,
        [
            split,
            split,
            # The response-time export for two contains only its domain. A
            # contradictory independent claim stays visible as a coverage gap.
            context(33002, "t5", "two", [Y]),
            context(33003, "t5", "three", []),
            context(33004, "known", "one", [X, Y]),
            context(33005, "known", "two", [DOMAIN]),
            context(33006, "conflict-a", "one", [X, Y]),
            context(33006, "conflict-b", "one", [X, Y]),
            context(33008, "conflict-a", "two", [DOMAIN]),
            context(33009, "missing-statement", "one", [X, Y]),
            context(33010, "missing-statement", "two", [DOMAIN]),
        ],
    )


def load_quiz_events(f):
    events = [
        statement(i, EVALUATED, "problem", problem, {"raw": points, "max": 2})
        for i, problem, points in [
            (33001, "one", 1),
            (33002, "two", 0),
            (33003, "three", 2),
            (33004, "one", 1),
            (33005, "two", 2),
            (33006, "one", 2),
            (33007, "one", 2),
            (33008, "two", 2),
            (33009, "one", 1),
        ]
    ]
    f.insert("xapi.xapi_events_all", events + [events[0]])
    # No additional whole sitting. Existing fixture Quiz 201 has one correct answer.
    practice_items(f, 201, [(0, 1, 2), (1, 0, 1)])
    sink(
        f,
        "measureup_exam_question_attempt",
        [
            dict(
                source_id="2010100",
                question_id="51100",
                subdomain_attempt_id="20101",
                score="0",
                status="unanswered",
            )
        ],
        timestamp="2026-11-13 10:00:00",
    )
    sink(
        f,
        "measureup_exam_attempt_subdomain",
        [dict(source_id="20102", exam_subdomain_id="512", attempt_domain_id="2010")],
    )


def verify_quiz(f):
    f.expect(
        "repeated child and redundant parent preserve one earned and two possible points",
        "SELECT subdomain_id, observed_items, observed_numerator, observed_denominator, "
        f"coverage_status, isNull(score) FROM {REPORT} WHERE {SCOPE} "
        "AND attempt_key='native:t5' AND subdomain_id IS NOT NULL ORDER BY subdomain_id",
        "2\t1\t0.5\t1\tincomplete_item_mapping\t1\n"
        "3\t1\t0.5\t1\tincomplete_item_mapping\t1",
    )
    f.expect(
        "conflicting historical claim and confirmed untagged problem retain their coverage bucket",
        f"SELECT observed_items, item_statuses, isNull(score) FROM {REPORT} WHERE {SCOPE} "
        "AND attempt_key='native:t5' AND domain_id IS NULL",
        "2\t['mapping_changed_or_unavailable','unmapped']\t1",
    )
    f.expect(
        "complete results score each child and keep the domain-only result outside weak subdomains",
        "SELECT ifNull(subdomain_id,'(domain)'), score, is_weak, coverage_status "
        f"FROM {REPORT} WHERE {SCOPE} AND attempt_key='native:known' ORDER BY ifNull(subdomain_id,'')",
        "(domain)\t1\t\\N\tdomain_only\n2\t0.5\t1\tscored\n3\t0.5\t1\tscored",
    )
    f.expect(
        "unattested and conflicting source IDs remain diagnostics and create no Quiz sitting",
        "SELECT link_status FROM reporting_pss_restricted.int_pss_native_quiz_item_attribution "
        f"WHERE {SCOPE} AND source_result_id IN ('{UUID(int=33006)}','{UUID(int=33007)}') "
        "ORDER BY source_result_id",
        "conflicting_sitting_context\nwhole_sitting_key_unavailable",
    )
    f.expect(
        "verified provider-native binary outcomes use the provider subdomain and one whole Quiz",
        "SELECT taxonomy_id, domain_id, subdomain_id, observed_numerator, observed_denominator, "
        f"score FROM {REPORT} WHERE {SCOPE} AND provider='measureup' AND subdomain_id='510'",
        "measureup:11\t500\t510\t1\t2\t0.5",
    )
    f.expect(
        "provider unanswered zero and a missing question branch carry coverage",
        f"SELECT subdomain_id, coverage_status, isNull(score) FROM {REPORT} WHERE {SCOPE} "
        "AND provider='measureup' AND subdomain_id IN ('511','512') ORDER BY subdomain_id",
        "511\titem_outcome_unavailable\t1\n512\tno_selected_items\t1",
    )
    f.expect(
        "a conflicted claim withholds both of its candidate sittings",
        f"SELECT count() FROM {REPORT} WHERE {SCOPE} AND startsWith(attempt_key,'native:conflict-')",
        "0",
    )
    f.expect(
        "a declared missing native statement withholds scores for its verified sibling",
        "SELECT count(), countIf(observed_numerator=0.5 AND observed_denominator=1 "
        "AND attempt_mapping_gaps=1 AND isNull(score)) "
        f"FROM {REPORT} WHERE {SCOPE} AND attempt_key='native:missing-statement'",
        "2\t2",
    )
    f.expect(
        "item and objective multiplicity leaves the provider Quiz count and score intact",
        "SELECT count(), any(score_pct) FROM reporting.rpt_pss_activity_attempts WHERE "
        f"{SCOPE} AND activity_type='Quiz'",
        "1\t60",
    )


def verify_observed_tag_history(f, refresh):
    # Export the full course each time. edited_on and old ObjectTag rows remain
    # unchanged when a problem is retagged or disappears in a later export.
    original = f.sql(
        "SELECT * FROM event_sink.course_blocks "
        f"WHERE course_key='{MASTER}' AND dump_id=toUUID('{UUID(int=1)}') FORMAT JSONEachRow"
    ).splitlines()
    problem = block("problem", "one")
    cases = [
        (33101, "retagged", "10:31:00", [1, 3]),
        (33102, "cleared", "10:32:00", []),
        (33103, "removed", "10:33:00", None),
    ]
    events = []
    for event_id, attempt, hour, tags in cases:
        cutoff = "2026-11-12 " + hour
        blocks = []
        for line in original:
            row = json.loads(line)
            if row["location"] == problem:
                if tags is None:
                    continue
                payload = json.loads(row["xblock_data_json"])
                payload["tags"] = tags
                row["xblock_data_json"] = json.dumps(payload)
            row.update(time_last_dumped=cutoff, dump_id=str(UUID(int=event_id)))
            blocks.append(row)
        f.insert("event_sink.course_blocks", blocks)
        # The adapter attests the selected item and sitting only. Mapping comes
        # from the export at the response time, after this sitting's start.
        claim = context(event_id, attempt, "one")
        f.insert(CONTEXT, [claim, {**claim, "objectives": [DOMAIN]}])
        event = statement(event_id, EVALUATED, "problem", "one", {"raw": 1, "max": 2})
        payload = json.loads(event["event"])
        payload["timestamp"] = cutoff.replace(" ", "T") + "Z"
        event["event"] = json.dumps(payload)
        events.append(event)
    # The enclosing course in xAPI is valid, but this usage key belongs to a
    # different CCX. The adapter cannot turn it into an eligible class item.
    f.insert(CONTEXT, [context(33104, "wrong-class-item", "one")])
    wrong_class = statement(33104, EVALUATED, "problem", "one", {"raw": 1, "max": 2})
    payload = json.loads(wrong_class["event"])
    payload["object"]["id"] = payload["object"]["id"].replace("+ccx@101+", "+ccx@102+")
    wrong_class["event"] = json.dumps(payload)
    # Independent attestations must also agree on the historical subsection.
    # Both subsection keys resolve to approved Quizzes for this master and CCX.
    wrong_placement = context(33105, "wrong-subsection", "one")
    wrong_placement["subsection_block_id"] = block("sequential", "ten")
    f.insert(CONTEXT, [wrong_placement, context(33106, "missing-time", "one")])
    events.append(statement(33105, EVALUATED, "problem", "one", {"raw": 1, "max": 2}))
    missing_time = statement(33106, EVALUATED, "problem", "one", {"raw": 1, "max": 2})
    payload = json.loads(missing_time["event"])
    del payload["timestamp"]
    missing_time["event"] = json.dumps(payload)
    f.insert("xapi.xapi_events_all", [*events, wrong_class, missing_time])
    refresh(f)
    evidence = "reporting_pss_restricted.int_pss_native_quiz_item_attribution"
    f.expect(
        "inclusive observed export tracks retagging, explicit empty, and later absent problem",
        "SELECT claimed_attempt_key, tag_evidence_status, mapping_status, "
        "toString(tag_capture_id) FROM " + evidence + f" WHERE {SCOPE} "
        "AND claimed_attempt_key IN ('native:retagged','native:cleared','native:removed') "
        "ORDER BY source_result_id",
        f"native:retagged\tobserved\tmapped\t{UUID(int=33101)}\n"
        f"native:cleared\tobserved\tunmapped\t{UUID(int=33102)}\n"
        f"native:removed\tproblem_absent\tmapping_at_attempt_unverified\t{UUID(int=33103)}",
    )
    f.expect(
        "a later removal keeps earlier objective scores and separates unknown from confirmed empty",
        "SELECT attempt_key, ifNull(subdomain_id,'-'), score, coverage_status "
        f"FROM {REPORT} WHERE {SCOPE} AND attempt_key IN "
        "('native:known','native:retagged','native:cleared','native:removed') "
        "ORDER BY attempt_key, ifNull(subdomain_id,'')",
        "native:cleared\t-\t\\N\tunmapped\n"
        "native:known\t-\t1\tdomain_only\n"
        "native:known\t2\t0.5\tscored\n"
        "native:known\t3\t0.5\tscored\n"
        "native:removed\t-\t\\N\tmapping_at_attempt_unverified\n"
        "native:retagged\t3\t0.5\tscored",
    )
    f.expect(
        "another class or another historical Quiz cannot claim the selected result",
        f"SELECT link_status FROM {evidence} WHERE {SCOPE} "
        "AND claimed_attempt_key IN ('native:wrong-class-item','native:wrong-subsection')",
        "item_link_unverified\nitem_link_unverified",
    )
    f.expect(
        "an absent xAPI response time cannot borrow the emission or sitting timestamp",
        f"SELECT tag_evidence_status, mapping_status, isNull(tag_capture_id) FROM {evidence} "
        f"WHERE {SCOPE} AND claimed_attempt_key='native:missing-time'",
        "response_time_unavailable\tmapping_at_attempt_unverified\t1",
    )
