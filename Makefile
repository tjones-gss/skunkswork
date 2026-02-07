.PHONY: test lint format build up down migrate shell coverage clean dev-setup test-docker

# Platform detection
ifeq ($(OS),Windows_NT)
    PYTHON ?= python
    PIP ?= pip
    RM_PYCACHE = for /d /r . %%d in (__pycache__) do @if exist "%%d" rd /s /q "%%d"
    RM_PYC = del /s /q *.pyc 2>nul
else
    PYTHON ?= python3
    PIP ?= pip3
    RM_PYCACHE = find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
    RM_PYC = find . -type f -name "*.pyc" -delete 2>/dev/null || true
endif

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
		--cov=agents --cov=contracts --cov=state --cov=db --cov=models --cov=middleware \
		--cov-report=term-missing \
		--cov-fail-under=85

dev-setup:
	$(PIP) install -r requirements.txt
	$(PIP) install ruff pre-commit
	npm ci
	npx playwright install chromium
	pre-commit install

test-docker:
	$(DOCKER) compose -f docker-compose.test.yml up --build --abort-on-container-exit

clean:
	$(RM_PYCACHE)
	$(RM_PYC)
	rm -rf .pytest_cache .mypy_cache .ruff_cache htmlcov .coverage coverage.xml
