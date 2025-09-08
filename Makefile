.PHONY: install lint test format clean

install:
	pip install -e .[dev]
	pre-commit install

lint:
	pre-commit run --all-files

test:
	pytest

format:
	pre-commit run --all-files

clean:
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
