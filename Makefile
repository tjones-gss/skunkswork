.PHONY: test lint format build up down migrate shell coverage clean

PYTHON ?= python
PYTEST ?= $(PYTHON) -m pytest
DOCKER ?= docker

test:
	$(PYTEST) tests/ -v

lint:
	ruff check .

format:
	ruff format .

build:
	$(DOCKER) build -t nam-pipeline .

up:
	$(DOCKER) compose up -d

down:
	$(DOCKER) compose down

migrate:
	$(PYTHON) scripts/init_db.py

shell:
	$(DOCKER) compose exec app /bin/bash

coverage:
	$(PYTEST) tests/ \
		--cov=agents --cov=contracts --cov=state --cov=db --cov=models \
		--cov-report=term-missing

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache .mypy_cache htmlcov .coverage coverage.xml
