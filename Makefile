.PHONY: install test lint check

install:
	python -m pip install -e ".[dev]"

test:
	pytest --cov=signalkit_stream --cov-report=term-missing --cov-fail-under=80

lint:
	ruff check .

check: lint test
	python -m compileall -q src
