#!/usr/bin/env python3
"""Validate the Course Licensing current-state dbt implementation.

This check is warehouse-independent. It protects the source contracts,
the deterministic latest-event ordering, tombstone handling, model
documentation, and unit-test coverage before dbt parses the complete
project in CI.

To add a new Course Licensing subdomain: add its entities to
ENTITY_AREAS, add a course_licensing_<entity>_columns() macro to
macros/course_licensing/entity_columns.sql, and append its
models/unit-tests YAML files to
MODEL_YAML_FILES/UNIT_TEST_YAML_FILES. Column contracts are not
duplicated here — validate_sources() reads them from that one shared
macro (the same one the mv_/latest_state models call) and cross-checks
it against the source YAML, so there is nothing else to keep in sync
by hand.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DOMAIN_ROOT = ROOT / "models/course_licensing"
CORE_ROOT = DOMAIN_ROOT / "core"
SOURCE_FILE = DOMAIN_ROOT / "sources/_course_licensing__sources.yml"
MACRO_FILE = ROOT / "macros/course_licensing/latest_state.sql"
ENTITY_COLUMNS_MACRO_FILE = ROOT / "macros/course_licensing/entity_columns.sql"

EXPECTED_SOURCE_NAME = "course_licensing_event_sink"

# Maps each entity to the models/course_licensing/core/<area>/ folder it lives
# in. This is the one registry that cannot be derived from anything else, so
# it is the canonical list of registered entities — add one entry per entity
# as new Course Licensing subdomains are implemented.
ENTITY_AREAS = {
    "institution": "institutions",
    "institution_administrator": "institutions",
    "license": "licenses",
    "license_order": "licenses",
    "institution_ccx": "classes",
    "licensed_enrollment": "enrollments",
    "course_enrollment_allowed": "enrollments",
    "course": "courses",
    "other_course_settings_cache": "courses",
    "instructor": "instructor_assignments",
    "instructor_institution": "instructor_assignments",
    "instructor_class": "instructor_assignments",
}

# One models/unit-tests YAML pair per functional area (not per entity, since
# several entities can share one area's YAML files).
MODEL_YAML_FILES = [
    CORE_ROOT / "institutions/_institutions__models.yml",
    CORE_ROOT / "licenses/_licenses__models.yml",
    CORE_ROOT / "classes/_classes__models.yml",
    CORE_ROOT / "enrollments/_enrollments__models.yml",
    CORE_ROOT / "courses/_courses__models.yml",
    CORE_ROOT / "instructor_assignments/_instructor_assignments__models.yml",
]
UNIT_TEST_YAML_FILES = [
    CORE_ROOT / "institutions/_institutions__unit_tests.yml",
    CORE_ROOT / "licenses/_licenses__unit_tests.yml",
    CORE_ROOT / "classes/_classes__unit_tests.yml",
    CORE_ROOT / "enrollments/_enrollments__unit_tests.yml",
    CORE_ROOT / "courses/_courses__unit_tests.yml",
    CORE_ROOT / "instructor_assignments/_instructor_assignments__unit_tests.yml",
]

# Business-level models that combine more than one entity and therefore
# don't fit the per-entity mv_/latest_state/current pattern model_paths()
# describes. Each still needs a documented model and at least one unit test,
# just like a per-entity current model — add one entry per such view as
# they're introduced.
ADDITIONAL_MODEL_NAMES = {
    "int_course_licensing_enrollment_current",
}


class ValidationError(RuntimeError):
    """Raised when a required Course Licensing invariant is not satisfied."""


def require(condition: bool, message: str) -> None:
    """Raise a validation error when an invariant is not satisfied."""
    if not condition:
        raise ValidationError(message)


def require_pattern(
    text: str, pattern: str, message: str, *, flags: int = re.IGNORECASE | re.DOTALL
) -> None:
    """Require a formatting-insensitive regular-expression match."""
    require(re.search(pattern, text, flags) is not None, message)


def forbid_pattern(text: str, pattern: str, message: str) -> None:
    """Forbid a regular-expression match (used to guard against known bugs)."""
    require(re.search(pattern, text, re.IGNORECASE) is None, message)


def load_yaml(path: Path) -> Any:
    """Load one YAML file and report its repository-relative path on failure."""
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ValidationError(f"Unable to load {path.relative_to(ROOT)}: {exc}") from exc


def model_paths(entity: str) -> tuple[Path, Path, Path]:
    """Return the materialized, finalized, and current model paths."""
    area = CORE_ROOT / ENTITY_AREAS[entity]
    return (
        area / f"mv_course_licensing_{entity}_latest_state.sql",
        area / f"int_course_licensing_{entity}_latest_state.sql",
        area / f"int_course_licensing_{entity}_current.sql",
    )


def entity_columns(entity: str) -> list[str]:
    """Return the entity's column contract from its shared macro definition."""
    text = ENTITY_COLUMNS_MACRO_FILE.read_text(encoding="utf-8")
    macro_name = f"course_licensing_{entity}_columns"
    match = re.search(
        rf"{{%-?\s*macro\s+{macro_name}\s*\(\s*\)\s*-?%}}"
        r"(?P<body>.*?)"
        r"{%-?\s*endmacro\s*-?%}",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    require(match is not None, f"Missing macro {macro_name}() in {ENTITY_COLUMNS_MACRO_FILE.name}")
    list_match = re.search(r"return\(\s*\[(?P<items>.*?)\]\s*\)", match.group("body"), re.DOTALL)
    require(list_match is not None, f"{macro_name}() does not return a column list")
    return re.findall(r"[\"']([^\"']+)[\"']", list_match.group("items"))


def validate_layout() -> str:
    """Validate the folders and files required by every registered entity.

    Checks existence only, not a total SQL-file count: business-level
    models in ADDITIONAL_MODEL_NAMES don't follow the fixed
    3-files-per-entity shape model_paths() describes, so a total count
    would need updating by hand every time one is added. Existence checks
    scale on their own instead.
    """
    required_paths = [
        SOURCE_FILE,
        MACRO_FILE,
        ENTITY_COLUMNS_MACRO_FILE,
        ROOT / "macros/course_licensing/tests/current_matches_latest_event.sql",
        *MODEL_YAML_FILES,
        *UNIT_TEST_YAML_FILES,
    ]
    for entity in ENTITY_AREAS:
        required_paths.extend(model_paths(entity))
    missing = [str(path.relative_to(ROOT)) for path in required_paths if not path.exists()]
    require(not missing, "Missing required files: " + ", ".join(missing))

    missing_additional = [
        name for name in ADDITIONAL_MODEL_NAMES if not any(CORE_ROOT.rglob(f"{name}.sql"))
    ]
    require(
        not missing_additional,
        "Missing required business-level model(s): " + ", ".join(sorted(missing_additional)),
    )
    return "validated the sources/core repository structure"


def validate_sources() -> str:
    """Validate that source and entity-columns-macro column lists agree.

    The sink contract lives in exactly one place per entity going forward:
    the course_licensing_<entity>_columns() macro, which is also what the
    mv_/latest_state models themselves call. This compares that single
    source of truth against the documented source YAML, instead of against
    a separate hardcoded copy.
    """
    document = load_yaml(SOURCE_FILE)
    sources = document.get("sources", [])
    matching = [source for source in sources if source.get("name") == EXPECTED_SOURCE_NAME]
    require(
        len(matching) == 1,
        f"Expected exactly one '{EXPECTED_SOURCE_NAME}' Course Licensing source",
    )
    source = matching[0]
    require(
        source.get("database") == "{{ env_var('ASPECTS_EVENT_SINK_DATABASE', 'event_sink') }}",
        "The source database must default to event_sink",
    )

    tables = {table.get("name"): table for table in source.get("tables", [])}
    expected_tables = {f"course_licensing_{entity}" for entity in ENTITY_AREAS}
    require(
        expected_tables.issubset(tables),
        "Source is missing table(s) for a registered entity: "
        + ", ".join(sorted(expected_tables - set(tables))),
    )

    for entity in ENTITY_AREAS:
        table_name = f"course_licensing_{entity}"
        table = tables[table_name]
        source_columns = [column.get("name") for column in table.get("columns", [])]
        macro_columns = ["source_id", *entity_columns(entity)]

        require(
            macro_columns == source_columns,
            f"{entity}: course_licensing_{entity}_columns() differs from the "
            f"{table_name} source contract",
        )
        require(
            table.get("loaded_at_field") == "time_last_dumped",
            f"{table_name} must use time_last_dumped as loaded_at_field",
        )

    return f"validated {len(ENTITY_AREAS)} event-sink source contract(s) against their models"


def validate_ordering_macro() -> str:
    """Ensure the shared latest-state ordering macro is deterministic and unbroken."""
    text = MACRO_FILE.read_text(encoding="utf-8")
    macro_match = re.search(
        r"{%-?\s*macro\s+course_licensing_latest_state_order\b.*?%}"
        r"(?P<body>.*?)"
        r"{%-?\s*endmacro\s*-?%}",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    require(macro_match is not None, "Missing course_licensing_latest_state_order macro")
    body = macro_match.group("body")

    ordered_fields = [
        r"time_last_dumped",
        r"source_updated_at",
        r"is_deleted\s*=\s*['\"]True['\"]",
        r"sink_event_id",
    ]
    last_end = 0
    for field_pattern in ordered_fields:
        match = re.search(field_pattern, body[last_end:], re.IGNORECASE)
        require(
            match is not None,
            "Event ordering must be time_last_dumped, source_updated_at, "
            "is_deleted, sink_event_id (in that order)",
        )
        last_end += match.end()

    forbid_pattern(
        body,
        r"toUInt8\s*\(\s*is_deleted\s*\)",
        "is_deleted must be compared as the string 'True'/'False', not toUInt8(is_deleted)",
    )
    return "validated deterministic ordering led by time_last_dumped"


def validate_models() -> list[str]:
    """Validate the three-model latest/current architecture for every entity."""
    for entity in ENTITY_AREAS:
        source_table = f"course_licensing_{entity}"
        materialized_path, latest_path, current_path = model_paths(entity)

        materialized = materialized_path.read_text(encoding="utf-8")
        for pattern, description in [
            (r"materialized\s*=\s*[\"']materialized_view[\"']", "materialized='materialized_view'"),
            (r"aspects\.get_engine\s*\(\s*[\"']AggregatingMergeTree\(\)[\"']", "aspects.get_engine('AggregatingMergeTree()')"),
            (r"course_licensing_argmax_state\s*\(", "course_licensing_argmax_state("),
            (
                rf"course_licensing_{entity}_columns\s*\(\s*\)",
                f"value_columns = course_licensing_{entity}_columns()",
            ),
            (
                rf"source\s*\(\s*[\"']{EXPECTED_SOURCE_NAME}[\"']\s*,\s*[\"']{source_table}[\"']",
                f"source('{EXPECTED_SOURCE_NAME}', '{source_table}')",
            ),
        ]:
            require_pattern(materialized, pattern, f"{materialized_path.name} is missing: {description}")
        require(
            "course_licensing_latest_state" in materialized,
            f"{materialized_path.name} is missing the course_licensing_latest_state tag",
        )

        latest = latest_path.read_text(encoding="utf-8")
        for pattern, description in [
            (r"course_licensing_argmax_merge\s*\(", "course_licensing_argmax_merge("),
            (
                rf"course_licensing_{entity}_columns\s*\(\s*\)",
                f"value_columns = course_licensing_{entity}_columns()",
            ),
            (rf"ref\s*\(\s*[\"']mv_course_licensing_{entity}_latest_state[\"']", f"ref('mv_course_licensing_{entity}_latest_state')"),
            (r"group\s+by\s+source_id", "group by source_id"),
        ]:
            require_pattern(latest, pattern, f"{latest_path.name} is missing: {description}")
        require(
            "course_licensing_latest_state" in latest,
            f"{latest_path.name} is missing the course_licensing_latest_state tag",
        )

        current = current_path.read_text(encoding="utf-8")
        require_pattern(
            current,
            rf"ref\s*\(\s*[\"']int_course_licensing_{entity}_latest_state[\"']",
            f"{current_path.name} is missing: ref('int_course_licensing_{entity}_latest_state')",
        )
        require_pattern(
            current,
            r"is_deleted\s*!=\s*['\"]True['\"]",
            f"{current_path.name} is missing: is_deleted != 'True'",
        )
        forbid_pattern(
            current,
            r"is_deleted\s*=\s*false\b",
            f"{current_path.name} must not compare is_deleted as a boolean literal",
        )
        require(
            "course_licensing_current" in current,
            f"{current_path.name} is missing the course_licensing_current tag",
        )
        require(
            re.search(r"\bsource\s*\(", current, re.IGNORECASE) is None,
            f"{current_path.name} must filter tombstones after latest-state finalization, "
            "not query the source directly",
        )

    return [
        f"validated {len(ENTITY_AREAS)} materialized-view latest-state model(s)",
        f"validated {len(ENTITY_AREAS)} argMaxMerge latest-state view(s)",
        f"validated {len(ENTITY_AREAS)} current view(s) with post-finalization tombstone filtering",
    ]


def load_models() -> dict[str, dict[str, Any]]:
    """Load all model properties into one name-indexed mapping."""
    models: dict[str, dict[str, Any]] = {}
    for path in MODEL_YAML_FILES:
        document = load_yaml(path)
        require(document.get("version") == 2, f"{path.name} must use version: 2")
        for model in document.get("models", []):
            name = model.get("name")
            require(name not in models, f"Duplicate model documentation for {name}")
            models[name] = model
    return models


def load_unit_tests() -> list[dict[str, Any]]:
    """Load all unit tests from every registered functional area."""
    tests: list[dict[str, Any]] = []
    for path in UNIT_TEST_YAML_FILES:
        document = load_yaml(path)
        tests.extend(document.get("unit_tests", []))
    return tests


def validate_documentation_and_tests() -> list[str]:
    """Validate model documentation and unit-test coverage for every entity."""
    models = load_models()
    expected_model_count = 3 * len(ENTITY_AREAS)
    require(
        len(models) >= expected_model_count,
        f"Expected documentation for at least {expected_model_count} models, found {len(models)}",
    )
    missing_additional_docs = ADDITIONAL_MODEL_NAMES - set(models)
    require(
        not missing_additional_docs,
        "Missing model documentation for: " + ", ".join(sorted(missing_additional_docs)),
    )

    for entity in ENTITY_AREAS:
        current_name = f"int_course_licensing_{entity}_current"
        expected_names = {
            f"mv_course_licensing_{entity}_latest_state",
            f"int_course_licensing_{entity}_latest_state",
            current_name,
        }
        require(expected_names.issubset(models), f"Model documentation is incomplete for {entity}")

        current = models[current_name]
        require(current.get("description"), f"{current_name} needs a description")
        columns = {column.get("name"): column for column in current.get("columns", [])}
        source_id_tests = set(columns.get("source_id", {}).get("data_tests", []))
        require(
            {"not_null", "unique"}.issubset(source_id_tests),
            f"{current_name}.source_id must be unique and not null",
        )
        require(
            any(
                isinstance(test, dict) and "course_licensing_current_matches_latest_event" in test
                for test in current.get("data_tests", [])
            ),
            f"{current_name} must carry the course_licensing_current_matches_latest_event cross-check",
        )

    unit_tests = load_unit_tests()
    require(
        len(unit_tests) >= len(ENTITY_AREAS),
        f"Expected at least one unit test per entity ({len(ENTITY_AREAS)}), found {len(unit_tests)}",
    )
    actual_tested_models = {test.get("model") for test in unit_tests}
    expected_tested_models = {
        f"int_course_licensing_{entity}_current" for entity in ENTITY_AREAS
    } | ADDITIONAL_MODEL_NAMES
    require(
        expected_tested_models.issubset(actual_tested_models),
        "Unit tests do not cover every registered current model: missing "
        + ", ".join(sorted(expected_tested_models - actual_tested_models)),
    )
    for test in unit_tests:
        require(test.get("given"), f"Unit test {test.get('name')} has no fixtures")
        require(
            test.get("expect", {}).get("rows") is not None,
            f"Unit test {test.get('name')} has no expected rows",
        )

    return [
        f"validated documentation for {len(models)} models",
        f"validated {len(unit_tests)} unit test(s)",
    ]


def validate_namespace() -> str:
    """Prevent deprecated Course Operations or legacy currentization names."""
    relevant_paths = [
        *CORE_ROOT.rglob("*.sql"),
        *CORE_ROOT.rglob("*.yml"),
        *CORE_ROOT.rglob("*.yaml"),
        *ROOT.glob("macros/course_licensing/**/*.sql"),
    ]
    for path in relevant_paths:
        text = path.read_text(encoding="utf-8")
        require(
            "course_operations_" not in text,
            f"Deprecated course_operations namespace found in {path.relative_to(ROOT)}",
        )
        require(
            "latest_observed" not in text and "reconciled_current" not in text,
            f"Legacy currentization naming found in {path.relative_to(ROOT)}",
        )
    return "validated the unified course_licensing namespace"


def validate() -> list[str]:
    """Run all warehouse-independent Course Licensing implementation checks."""
    checks = [
        validate_layout(),
        validate_sources(),
        validate_ordering_macro(),
    ]
    checks.extend(validate_models())
    checks.extend(validate_documentation_and_tests())
    checks.append(validate_namespace())
    return checks


def main() -> int:
    """Run validation and return a process exit status."""
    try:
        checks = validate()
    except (ValidationError, OSError, yaml.YAMLError, re.error) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1

    for check in checks:
        print(f"PASS: {check}")
    print(f"PASS: {len(checks)} Course Licensing CI checks completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
