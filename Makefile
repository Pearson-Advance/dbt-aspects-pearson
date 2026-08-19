.PHONY: install deps validate parse format format-check clean

DBT_PROFILES_DIR ?= .github

install:
	python -m pip install -r requirements-dev.txt

deps:
	dbt deps

validate:
	python scripts/validate_project.py

parse: deps validate
	dbt parse --profiles-dir "$(DBT_PROFILES_DIR)" --no-partial-parse

format:
	@if find models macros tests analyses -type f -name '*.sql' -print -quit | grep -q .; then \
		sqlfmt models macros tests analyses; \
	else \
		echo "No SQL files to format."; \
	fi

format-check:
	@if find models macros tests analyses -type f -name '*.sql' -print -quit | grep -q .; then \
		sqlfmt models macros tests analyses --check; \
	else \
		echo "No SQL files to check."; \
	fi

clean:
	dbt clean
