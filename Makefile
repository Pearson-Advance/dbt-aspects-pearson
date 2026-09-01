.PHONY: install deps validate parse format format-check clean

DBT_PROFILES_DIR ?= .github
SQLFMT_PATHS := $(wildcard models macros tests analyses)

install:
	python -m pip install -r requirements-dev.txt

deps:
	dbt deps

validate:
	python scripts/validate_project.py

parse: deps validate
	dbt parse --profiles-dir "$(DBT_PROFILES_DIR)" --no-partial-parse

format:
	@if find $(SQLFMT_PATHS) -type f -name '*.sql' -print -quit | grep -q .; then \
		sqlfmt $(SQLFMT_PATHS); \
	else \
		echo "No SQL files to format."; \
	fi

format-check:
	@if find $(SQLFMT_PATHS) -type f -name '*.sql' -print -quit | grep -q .; then \
		sqlfmt $(SQLFMT_PATHS) --check; \
	else \
		echo "No SQL files to check."; \
	fi

clean:
	dbt clean
