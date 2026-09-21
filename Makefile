.PHONY: install edit run test check evidence docker-run docker-test

install:
	uv sync --frozen

edit:
	uv run marimo edit notebook.py

run:
	uv run marimo run notebook.py

test:
	uv run pytest

check:
	uv run ruff check notebook.py tests scripts
	uv run marimo check --strict notebook.py

evidence:
	uv run python scripts/make_evidence.py

docker-run:
	docker compose up --build notebook

docker-test:
	docker compose run --rm --build notebook /app/.venv/bin/python -m pytest -q
