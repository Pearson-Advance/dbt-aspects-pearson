"""Build the dbt reporting graph and test it twice against disposable ClickHouse data."""

import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import yaml

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
DB = "reporting_fixture"
UPSTREAM_MVS = (
    "dim_course_blocks",
    "rpt_pss_activity_attempts",
    "rpt_pss_activity_scores_current",
    "rpt_pss_learner_readiness_current",
    "rpt_pss_practice_subdomain_results",
)
REPORT_MVS = (
    "int_pss_class_catalog_coverage",
    "rpt_pss_learner_progress",
    "rpt_pss_class_activity_delivery",
    "rpt_pss_class_metrics",
)


def fixture_project(project, port):
    project.mkdir()
    for config in ("dbt_project.yml", "packages.yml"):
        shutil.copy(ROOT / config, project)
    for path in ("macros", "models/pss_insights/reporting", "tests/pss_insights/reporting"):
        shutil.copytree(ROOT / path, project / path)
    # The real PADV-3834 opaque key macros are needed by the navigation model.
    usage_keys = project / "macros/pss_insights/usage_keys.sql"
    if not usage_keys.exists():
        revision = os.environ.get(
            "PSS_3834_REV", "afff849e14da7b28695bbfd9cb1972b251a4bf97"
        )
        usage_keys.write_bytes(
            subprocess.check_output(
                ["git", "show", f"{revision}:macros/pss_insights/usage_keys.sql"],
                cwd=ROOT,
            )
        )

    # Load the actual pinned adapter/package macros. Replace all upstream models
    # with local, explicitly typed examples so no LMS or provider data is needed.
    packages = project / "dbt_packages"
    packages.mkdir()
    for dep in ("codegen", "dbt_utils"):
        (packages / dep).symlink_to(
            ROOT / "dbt_packages" / dep, target_is_directory=True
        )
    aspects = packages / "aspects"
    aspects.mkdir()
    shutil.copy(ROOT / "dbt_packages/aspects/dbt_project.yml", aspects)
    (aspects / "macros").symlink_to(
        ROOT / "dbt_packages/aspects/macros", target_is_directory=True
    )
    inputs = project / "models/fixture_inputs"
    inputs.mkdir()
    for name, sql in yaml.safe_load((HERE / "upstream.yml").read_text()).items():
        if name in UPSTREAM_MVS:
            sql = (
                "{{ config(materialized='materialized_view', engine='MergeTree()', "
                "order_by='tuple()', refreshable={'interval':'EVERY 4 HOUR'}) }}\n"
            ) + sql
        (inputs / f"{name}.sql").write_text(sql)
    profile = yaml.safe_load((ROOT / ".github/profiles.yml").read_text())
    profile["aspects"]["outputs"]["ci"].update(host="127.0.0.1", port=port, schema=DB)
    (project / "profiles.yml").write_text(yaml.safe_dump(profile))


def exercise(project, url):
    variables = dict(
        pss_reporting_enabled=True,
        pss_class_coverage_model="fixture_class_coverage",
        pss_activity_coverage_model="fixture_activity_coverage",
    )
    dbt = [
        os.environ.get("DBT", "dbt"),
        "build",
        "--profiles-dir",
        str(project),
        "--vars",
        json.dumps(variables),
        "--select",
        "package:dbt_aspects_pearson",
    ]
    subprocess.run(
        dbt,
        cwd=project,
        check=True,
        env={
            **os.environ,
            "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
            "ASPECTS_EVENT_SINK_DATABASE": "event_sink",
        },
    )

    def query(sql):
        with urlopen(url, data=sql.encode(), timeout=45) as response:
            return response.read().decode().strip()

    # Check the storage definitions as well as the observed results. Manual
    # refresh order alone would hide an omitted DEPENDS ON in the scheduled refresh graph.
    required_edges = {
        "int_pss_class_catalog_coverage": ["dim_course_blocks"],
        "rpt_pss_class_activity_delivery": ["int_pss_class_catalog_coverage"],
        "rpt_pss_class_metrics": [
            "rpt_pss_class_activity_delivery",
            "rpt_pss_learner_progress",
        ],
    }
    for child, parents in required_edges.items():
        ddl = query(f"SHOW CREATE TABLE {DB}.{child}_mv").replace("`", "")
        dependencies = ddl.partition("DEPENDS ON ")[2].partition(" TO ")[0]
        for parent in parents:
            if f"{DB}.{parent}_mv" not in dependencies:
                raise ValueError(f"Missing refresh dependency {parent} on {child}")

    for cycle in range(2):
        for model in (*UPSTREAM_MVS, *REPORT_MVS):
            for action in ("REFRESH", "WAIT"):
                query(f"SYSTEM {action} VIEW {DB}.{model}_mv")
        result = query(
            f"SELECT registered_learners, class_risk, final_pass_rate, "
            f"total_activities, completed_activities, remaining_activities, "
            f"avg_course_grade, avg_content_progress FROM {DB}.rpt_pss_class_metrics"
        )
        expected = "1\tLow\t1\t2\t1\t1\t0.8\t1"
        if result != expected:
            raise ValueError(
                f"Refresh {cycle + 1}: expected {expected!r}, got {result!r}"
            )
        print(
            f"Refresh {cycle + 1}: {len(UPSTREAM_MVS) + len(REPORT_MVS)} views refreshed; class totals reconciled."
        )


def main():
    with tempfile.TemporaryDirectory(prefix="pss-reporting-") as scratch:
        directory = Path(scratch)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        project = directory / "project"
        fixture_project(project, port)
        config = HERE / "server.xml"
        env = {
            **os.environ,
            "PSS_FIXTURE_PORT": str(port),
            "PSS_FIXTURE_DATA": f"{directory}/data/",
            "PSS_FIXTURE_USERS": str(config),
        }
        url = f"http://127.0.0.1:{port}/"
        with (directory / "server.log").open("w+") as log:
            server = subprocess.Popen(
                [
                    os.environ.get("CLICKHOUSE", "clickhouse"),
                    "server",
                    "--config-file",
                    str(config),
                ],
                cwd=directory,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            try:
                for _ in range(100):
                    if server.poll() is not None:
                        log.seek(0)
                        raise RuntimeError(log.read())
                    try:
                        with urlopen(url + "ping", timeout=1):
                            break
                    except URLError:
                        time.sleep(0.1)
                else:
                    raise TimeoutError("Fixture ClickHouse did not start")
                exercise(project, url)
            finally:
                server.terminate()
                server.wait(timeout=30)


if __name__ == "__main__":
    main()
