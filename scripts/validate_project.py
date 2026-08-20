#!/usr/bin/env python3
"""Validate the Ulmo/Aspects compatibility contract for this dbt project."""

from __future__ import annotations

import sys
import tomllib
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_ASPECTS_REVISION = "v8.0.0"
EXPECTED_ASPECTS_COMMIT = "a437d09497507b7eac17d9a7b18a0f8a93ecff93"
EXPECTED_LOCK_HASH = "d183b7c6075e86b92885d4363d9744ddc20f5ec5"
EXPECTED_RUNTIME_REQUIREMENTS = {
    "dbt-clickhouse==1.9.4",
    "dbt-core~=1.9.0",
}
EXPECTED_DBT_RANGE = [">=1.9.0", "<1.10.0"]


class ValidationError(RuntimeError):
    """Raised when a required project invariant is not satisfied."""


def load_yaml(relative_path: str) -> Any:
    """Load a YAML file and report a useful path on failure."""
    path = ROOT / relative_path
    try:
        with path.open(encoding="utf-8") as stream:
            return yaml.safe_load(stream)
    except (OSError, yaml.YAMLError) as exc:
        raise ValidationError(f"Unable to load {relative_path}: {exc}") from exc


def requirement_lines(relative_path: str) -> set[str]:
    """Return non-empty, non-comment requirement lines."""
    path = ROOT / relative_path
    try:
        return {
            line.strip()
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        }
    except OSError as exc:
        raise ValidationError(f"Unable to read {relative_path}: {exc}") from exc


def find_aspects_package(packages: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Find the openedx/aspects-dbt Git package declaration."""
    for package in packages:
        if package.get("git") == "https://github.com/openedx/aspects-dbt.git":
            return package
    return None


def validate_all_yaml() -> int:
    """Parse every committed YAML file outside generated directories."""
    ignored_parts = {".git", "dbt_packages", "target", "logs"}
    yaml_paths = sorted(
        path
        for pattern in ("*.yml", "*.yaml")
        for path in ROOT.rglob(pattern)
        if ignored_parts.isdisjoint(path.parts)
    )
    for path in yaml_paths:
        try:
            with path.open(encoding="utf-8") as stream:
                yaml.safe_load(stream)
        except (OSError, yaml.YAMLError) as exc:
            raise ValidationError(
                f"Invalid YAML in {path.relative_to(ROOT)}: {exc}"
            ) from exc
    return len(yaml_paths)


def validate() -> list[str]:
    """Validate all repository invariants and return human-readable checks."""
    checks: list[str] = []

    yaml_count = validate_all_yaml()
    checks.append(f"parsed {yaml_count} YAML files")

    project = load_yaml("dbt_project.yml")
    if project.get("name") != "dbt_aspects_pearson":
        raise ValidationError("dbt_project.yml must use name=dbt_aspects_pearson")
    if project.get("profile") != "aspects":
        raise ValidationError("dbt_project.yml must use profile=aspects")
    if project.get("target-path") != "target":
        raise ValidationError("dbt_project.yml must use target-path=target")
    if project.get("require-dbt-version") != EXPECTED_DBT_RANGE:
        raise ValidationError(
            "dbt_project.yml require-dbt-version must match dbt 1.9.x"
        )
    model_config = project.get("models", {}).get("dbt_aspects_pearson", {})
    if "example" in model_config:
        raise ValidationError("Remove the stale dbt starter 'example' model config")
    if model_config.get("+materialized") != "view":
        raise ValidationError("Project-wide default materialization must remain 'view'")
    checks.append("validated dbt project configuration")

    packages = load_yaml("packages.yml").get("packages", [])
    aspects_package = find_aspects_package(packages)
    if not aspects_package:
        raise ValidationError("packages.yml must declare openedx/aspects-dbt")
    if aspects_package.get("revision") != EXPECTED_ASPECTS_REVISION:
        raise ValidationError(
            f"packages.yml must pin aspects-dbt {EXPECTED_ASPECTS_REVISION}"
        )
    checks.append(f"validated aspects-dbt revision {EXPECTED_ASPECTS_REVISION}")

    package_lock = load_yaml("package-lock.yml")
    locked_aspects = find_aspects_package(package_lock.get("packages", []))
    if not locked_aspects:
        raise ValidationError("package-lock.yml must lock openedx/aspects-dbt")
    if locked_aspects.get("revision") != EXPECTED_ASPECTS_COMMIT:
        raise ValidationError("package-lock.yml has an unexpected aspects-dbt commit")
    if package_lock.get("sha1_hash") != EXPECTED_LOCK_HASH:
        raise ValidationError("package-lock.yml has an unexpected dependency hash")
    checks.append("validated locked upstream commit and transitive packages")

    runtime_requirements = requirement_lines("requirements.txt")
    missing_runtime_requirements = (
        EXPECTED_RUNTIME_REQUIREMENTS - runtime_requirements
    )
    if missing_runtime_requirements:
        raise ValidationError(
            "requirements.txt is missing the Ulmo runtime dbt pins: "
            + ", ".join(sorted(missing_runtime_requirements))
        )
    conflicting_dbt_requirements = {
        requirement
        for requirement in runtime_requirements
        if requirement.startswith(("dbt-core", "dbt-clickhouse"))
        and requirement not in EXPECTED_RUNTIME_REQUIREMENTS
    }
    if conflicting_dbt_requirements:
        raise ValidationError(
            "requirements.txt contains conflicting dbt pins: "
            + ", ".join(sorted(conflicting_dbt_requirements))
        )
    dev_requirements = requirement_lines("requirements-dev.txt")
    if "-r requirements.txt" not in dev_requirements:
        raise ValidationError("requirements-dev.txt must include requirements.txt")
    if "shandy-sqlfmt[jinjafmt]==0.26.0" not in dev_requirements:
        raise ValidationError("requirements-dev.txt must pin upstream sqlfmt 0.26.0")
    checks.append("validated runtime and development dependencies")

    compatibility = load_yaml("compatibility.yml").get("compatibility", {})
    expected_values = {
        "openedx_release": "ulmo",
        "tutor_contrib_aspects": "3.0.3",
    }
    for key, expected in expected_values.items():
        if compatibility.get(key) != expected:
            raise ValidationError(f"compatibility.yml {key} must be {expected}")
    if compatibility.get("tutor", {}).get("major") != 21:
        raise ValidationError("compatibility.yml Tutor major must be 21")
    aspects_compatibility = compatibility.get("aspects_dbt", {})
    if aspects_compatibility.get("revision") != EXPECTED_ASPECTS_REVISION:
        raise ValidationError("compatibility.yml has an unexpected aspects-dbt revision")
    if aspects_compatibility.get("resolved_commit") != EXPECTED_ASPECTS_COMMIT:
        raise ValidationError("compatibility.yml has an unexpected aspects-dbt commit")
    runtime = compatibility.get("runtime", {})
    expected_runtime = {
        "python": "3.12",
        "clickhouse_server": "25.8",
        "dbt_core": ">=1.9.0,<1.10.0",
        "dbt_clickhouse": "1.9.4",
    }
    for key, expected in expected_runtime.items():
        if runtime.get(key) != expected:
            raise ValidationError(
                f"compatibility.yml runtime.{key} must be {expected}"
            )
    expected_databases = {
        "target": "reporting",
        "event_sink": "event_sink",
        "xapi": "xapi",
    }
    databases = compatibility.get("databases", {})
    for key, expected in expected_databases.items():
        if databases.get(key) != expected:
            raise ValidationError(
                f"compatibility.yml databases.{key} must be {expected}"
            )
    checks.append("validated Ulmo/Aspects compatibility matrix")

    if (ROOT / ".python-version").read_text(encoding="utf-8").strip() != "3.12":
        raise ValidationError(".python-version must select Python 3.12")

    with (ROOT / "pyproject.toml").open("rb") as stream:
        pyproject = tomllib.load(stream)
    if pyproject.get("tool", {}).get("sqlfmt", {}).get("dialect") != "clickhouse":
        raise ValidationError("pyproject.toml must configure sqlfmt for ClickHouse")
    checks.append("validated Python and SQL formatter configuration")

    required_paths = [
        ".github/profiles.yml",
        ".github/workflows/dbt-static-checks.yml",
        "models/course_licensing",
        "models/measureup",
        "models/shared",
    ]
    missing = [path for path in required_paths if not (ROOT / path).exists()]
    if missing:
        raise ValidationError("Missing required paths: " + ", ".join(missing))
    if (ROOT / "profiles.yml").exists():
        raise ValidationError("Do not commit a root profiles.yml containing credentials")
    checks.append("validated CI and domain-ready project layout")

    workflow = (ROOT / ".github/workflows/dbt-static-checks.yml").read_text(
        encoding="utf-8"
    )
    required_workflow_fragments = [
        'python-version: "3.12"',
        "python scripts/validate_project.py",
        "dbt deps",
        "dbt parse --no-partial-parse",
        "make format-check",
    ]
    missing_fragments = [
        fragment for fragment in required_workflow_fragments if fragment not in workflow
    ]
    if missing_fragments:
        raise ValidationError(
            "CI workflow is missing checks: " + ", ".join(missing_fragments)
        )
    checks.append("validated CI command coverage")

    return checks


def main() -> int:
    """Run validation and return a process exit code."""
    try:
        checks = validate()
    except (ValidationError, OSError, tomllib.TOMLDecodeError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    for check in checks:
        print(f"PASS: {check}")
    print("PASS: repository is reconciled for the Ulmo/Aspects 3.0.3 baseline")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
