"""Exercise interval certification using the public macro on actual attempt facts."""

from sources import CLASS_A, CLASS_B, attempt, block, sink


def verify_period(f):
    f.dbt("compile", "--select", "pss_attempt_period_reconciliation")
    compiled = f.scratch / "target/compiled/dbt_aspects_pearson/analyses"
    query = (compiled / "pss_attempt_period_reconciliation.sql").read_text()
    final = block("sequential", "final")

    def statuses():
        return (
            "SELECT observed_participation_count, period_status, "
            "ifNull(toString(participation_attempt_count), 'unknown') "
            f"FROM ({query}) WHERE subsection_block_id='{final}' ORDER BY ccx_id"
        )

    f.expect(
        "uncertified interval preserves observed participation",
        statuses(),
        "2\tunverified_interval\tunknown\n0\tunverified_interval\tunknown",
    )
    certificates = []
    for ccx_id, bounds in [
        (CLASS_A, [("10:00", "10:30"), ("10:30", "11:00")]),
        (CLASS_B, [("10:00", "10:25"), ("10:35", "11:00")]),
    ]:
        for start, end in bounds:
            certificates.append(
                dict(
                    ccx_id=ccx_id,
                    subsection_block_id=final,
                    snapshot_id=f"{1:024x}",
                    coverage_start=f"2026-11-02 {start}:00",
                    coverage_end=f"2026-11-02 {end}:00",
                    verified_at="2026-11-03 12:00:00",
                    evidence_ref="fixture-check",
                )
            )
    table = "reporting.pss_attempt_coverage_evidence"
    f.insert(table, certificates)
    f.expect(
        "start inclusive, end exclusive and ten-minute feed gap",
        statuses(),
        "2\tverified\t2\n0\tunverified_interval\tunknown",
    )
    f.insert(
        table,
        [
            {
                **certificates[-1],
                "coverage_start": "2026-11-02 10:25:00",
                "coverage_end": "2026-11-02 10:35:00",
            }
        ],
    )
    f.expect(
        "complete feed certifies zero in the other class",
        statuses(),
        "2\tverified\t2\n0\tverified\t0",
    )

    # The provider omitted the start on a corrected copy of an existing sitting.
    # A bounded certificate cannot place that source sitting inside or outside the window.
    mv = "reporting.rpt_pss_activity_attempts_mv"
    for row, dumped_at, expected in [
        (
            attempt(101, 11, "60", started=""),
            "2026-11-03 10:00:00",
            "1\tunresolved_attempts\tunknown\n0\tverified\t0",
        ),
        (
            attempt(101, 11, "60"),
            "2026-11-03 10:01:00",
            "2\tverified\t2\n0\tverified\t0",
        ),
    ]:
        sink(f, "measureup_exam_test_attempt", [row], dumped_at)
        f.sql("SYSTEM REFRESH VIEW " + mv)
        f.sql("SYSTEM WAIT VIEW " + mv)
        f.expect(
            "period eligibility after a timestamp correction", statuses(), expected
        )
