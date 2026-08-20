# Pearson Aspects dbt package

This repository is Pearson's custom dbt package for Open edX Aspects. It installs the upstream `openedx/aspects-dbt` project and provides a controlled location for Pearson-owned reporting models, including the planned Course Licensing and MeasureUp current-state materialized views.

## Supported deployment baseline

| Component | Reconciled version |
|---|---:|
| Open edX release | Ulmo |
| Tutor | 21.x |
| `tutor-contrib-aspects` | 3.0.3 |
| Upstream `aspects-dbt` | 8.0.0 |
| Python | 3.12 |
| `dbt-core` | `>=1.9.0,<1.10.0` |
| `dbt-clickhouse` | 1.9.4 |
| ClickHouse | 25.8 |
| dbt target database | `reporting` |

See `compatibility.yml` for the machine-readable contract and `RECONCILIATION.md` for the decisions behind it.

### Why the Aspects stack is the dependency authority

This repository is installed inside the dedicated Aspects dbt image, not inside the LMS/CMS Python environment. Therefore, its Python and dbt dependencies must match the installed `tutor-contrib-aspects` release rather than the `edx-platform` requirements files. For Aspects 3.0.3, the plugin selects upstream `aspects-dbt` 8.0.0 and builds the dbt image on Python 3.12.

Do not independently move `packages.yml` to a newer `aspects-dbt` tag. Upgrade the custom package only as part of an explicit Aspects compatibility change.

## Repository structure

```text
.
├── models/
│   ├── course_licensing/   # Pearson Course Licensing models
│   ├── measureup/          # Pearson MeasureUp models
│   └── shared/             # Cross-domain intermediate models
├── scripts/
│   └── validate_project.py # Static compatibility guard
├── compatibility.yml       # Supported deployment matrix
├── package-lock.yml        # Resolved dbt package revisions
├── packages.yml            # Upstream aspects-dbt declaration
├── requirements.txt        # Runtime dependencies used by the Aspects image
└── requirements-dev.txt    # Local/CI formatting dependencies
```

Modeling rules and the required current-state contract are documented in `models/README.md`.

## Local setup

Use Python 3.12 to mirror the Aspects image:

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

Create a local dbt profile without committing credentials:

```bash
mkdir -p ~/.dbt
cp sample_profiles.yml ~/.dbt/profiles.yml
```

The sample targets the `reporting` database used by Aspects. Set `CLICKHOUSE_PASSWORD` and override the other connection variables when they differ from the sample defaults:

```bash
export CLICKHOUSE_PASSWORD='<local-clickhouse-admin-password>'
python scripts/validate_project.py
dbt deps
dbt debug
dbt parse --no-partial-parse
```

Useful make targets:

```bash
make validate
make deps
make DBT_PROFILES_DIR="$HOME/.dbt" parse
make format
make format-check
```

## Use from Tutor

Configure Tutor to clone this repository instead of upstream `aspects-dbt`. Use an immutable release tag or commit for `DBT_BRANCH`:

```bash
tutor config save \
  --set "DBT_REPOSITORY=https://github.com/Pearson-Advance/dbt-aspects-pearson.git" \
  --set "DBT_BRANCH=replace-with-immutable-tag-or-commit"
```

For a private repository, configure `DBT_SSH_KEY` and use the SSH clone URL according to the Aspects deployment's secret-management procedure.

Rebuild the dbt image after changing the repository revision or Python requirements:

```bash
tutor images build aspects --no-cache
```

Validate the package in the target environment before running transformations:

```bash
tutor dev do dbt -c "build"
tutor dev do dbt -c "test --selector unit_tests"
tutor dev do dbt -c "test --selector non_unit_tests"
```

Use the equivalent `tutor local do ...` commands for a local/production deployment rather than the Tutor development environment.

Tutor fetches a clean copy of the configured repository and revision. Local uncommitted changes are therefore not visible to a dbt job.

## Adding custom models

Place each model under its business domain and define its grain before writing the SQL. A production current-state model should normally include:

- deterministic latest-record selection per stable business key;
- explicit handling of sink deletion/tombstone records;
- a stable tie-breaker for records with equal source timestamps;
- documented ClickHouse engine, primary key, `ORDER BY`, partitioning, and replacement/version semantics;
- schema documentation, generic data tests, and dbt unit tests;
- `ref()` dependencies where an upstream Aspects model already represents the required concept.

The project defaults to a normal view. A model intended to be a ClickHouse materialized view must opt in explicitly:

```sql
{{
    config(
        materialized="materialized_view",
        engine=aspects.get_engine("ReplacingMergeTree()"),
        primary_key="(entity_id)",
        order_by="(entity_id)",
    )
}}
```

Do not copy that configuration without checking the entity's update pattern. In particular, current-state tables backed by event sinks need a deliberate strategy for late events, duplicate dumps, and deletions.

## Validation and CI

The repository includes two layers of protection:

1. `scripts/validate_project.py` checks the Ulmo compatibility matrix, dbt package revision and lock, runtime requirements, project settings, and required folder/CI structure.
2. GitHub Actions installs the Python 3.12 toolchain, resolves the locked dbt packages, parses this project together with upstream `aspects-dbt`, and checks ClickHouse SQL formatting.

Run the same checks before opening a pull request:

```bash
make validate
make parse
make format-check
```

## Controlled upgrade policy

An Aspects/dbt upgrade must change the compatibility set atomically:

- installed `tutor-contrib-aspects` version;
- upstream `aspects-dbt` tag and lockfile commit;
- Python and ClickHouse image versions where changed upstream;
- `dbt-core` and `dbt-clickhouse` constraints;
- CI runtime and validation contract.

After the static checks pass, test the upgrade against production-shaped event-sink and xAPI data. Run full refreshes only when the upstream migration or materialization change requires them.


## Upstream references

- Open edX Aspects guide for extending dbt: <https://docs.openedx.org/projects/openedx-aspects/en/latest/technical_documentation/how-tos/dbt_extensions.html>
- `tutor-contrib-aspects` 3.0.3 configuration: <https://github.com/openedx/tutor-contrib-aspects/tree/v3.0.3>
- Upstream `aspects-dbt` 8.0.0: <https://github.com/openedx/aspects-dbt/tree/v8.0.0>
- Official custom-package example: <https://github.com/openedx/sample-aspects-dbt>
