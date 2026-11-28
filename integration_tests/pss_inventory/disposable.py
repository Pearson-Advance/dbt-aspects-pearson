"""Provision a private ClickHouse using the same sink columns as the Ulmo deployment."""

import argparse
from contextlib import contextmanager
import os
from pathlib import Path
import re
import subprocess
import time
from urllib.error import URLError
from urllib.request import urlopen


def schema_paths(description):
    parser = argparse.ArgumentParser(description=description)
    workspace = Path(__file__).resolve().parents[3]
    for flag, variable, package in (
        ("--tutor-pearson", "TUTOR_SCHEMA_DIR", "tutor_pearson_plugin"),
        ("--tutor-aspects", "ASPECTS_SCHEMA_DIR", "tutoraspects"),
    ):
        default = os.getenv(variable)
        if not default:
            roots = [p.parent for p in workspace.glob(f"*/{package}") if p.is_dir()]
            if len(roots) > 1:
                # Prefer the primary checkout over additional linked worktrees.
                roots = [root for root in roots if (root / ".git").is_dir()]
            default = roots[0] if len(roots) == 1 else None
        parser.add_argument(
            flag, type=Path, default=default, required=default is None,
            metavar="PATH", help=f"Directory containing {package} (env: {variable})",
        )
    return parser.parse_args()


def table_statement(source, table):
    match = re.search(
        rf"CREATE TABLE IF NOT EXISTS event_sink\.{table}\s*\(.*?;",
        source,
        re.DOTALL,
    )
    if not match:
        raise ValueError(f"Missing deployment DDL for {table}")
    return match.group()


def source_tables(pearson, aspects):
    ddl = (pearson / "tutor_pearson_plugin/services/aspects/defaults.py").read_text()
    for table in (
        "course_licensing_institution",
        "course_licensing_license",
        "course_licensing_institution_ccx",
        "course_licensing_course",
        "pss_subsection_assignment_types",
    ):
        yield table_statement(ddl, table)
    migrations = (
        aspects
        / "tutoraspects/templates/aspects/apps/aspects/migrations/alembic/versions"
    )
    # The pinned Tutor 3.0.3 migrations target either a local or a replicated
    # ClickHouse. Resolve their single-node placeholders for this fixture.
    for filename, engine, tables in [
        ("0007_event_sink.py", "MergeTree", ["course_overviews", "course_blocks"]),
        (
            "0039_event_sink_tag_taxonomy.py",
            "ReplacingMergeTree",
            ["tag", "taxonomy", "object_tag"],
        ),
    ]:
        source = (
            (migrations / filename)
            .read_text()
            .replace("{{ ASPECTS_EVENT_SINK_DATABASE }}", "event_sink")
        )
        source = source.replace("{on_cluster}", "").replace("{engine}", engine)
        for table in tables:
            yield table_statement(source, table)


@contextmanager
def database(pearson: Path, aspects: Path):
    statements = list(source_tables(pearson, aspects))
    server = subprocess.check_output(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--publish",
            "127.0.0.1::8123",
            "--env",
            "CLICKHOUSE_SKIP_USER_SETUP=1",
            "clickhouse/clickhouse-server:25.8",
        ],
        text=True,
    ).strip()
    try:
        address = subprocess.check_output(
            ["docker", "port", server, "8123/tcp"], text=True
        ).strip()
        url = f"http://{address}/"
        for _ in range(120):
            try:
                with urlopen(url + "ping", timeout=1):
                    break
            except (URLError, ConnectionError):
                time.sleep(0.25)
        else:
            raise TimeoutError("Disposable ClickHouse did not start")
        for sql in ["CREATE DATABASE event_sink", *statements]:
            with urlopen(url, data=sql.encode(), timeout=30):
                pass
        yield int(address.rsplit(":", 1)[1])
    finally:
        subprocess.run(
            ["docker", "stop", server], check=True, stdout=subprocess.DEVNULL
        )
