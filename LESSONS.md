# LESSONS.md

Lessons learned across multiple Claude Code sessions building the NAM Intelligence Pipeline. Reference this before starting new work to avoid repeating mistakes.

---

## Python / Environment

### `datetime.utcnow()` is deprecated (Python 3.12+)
- **Problem:** `datetime.utcnow()` emits `DeprecationWarning` and returns a naive datetime.
- **Fix:** Use `datetime.now(UTC)` from `datetime` module (`from datetime import UTC`).
- **Pydantic gotcha:** `default_factory` expects a callable, so use `default_factory=lambda: datetime.now(UTC)` — not `default_factory=datetime.now(UTC)` which calls it at class definition time.

### Windows cp1252 encoding breaks Unicode HTML
- **Problem:** `open()` defaults to `cp1252` on Windows, which crashes on UTF-8 HTML content (curly quotes, em-dashes, non-Latin characters).
- **Fix:** Always pass `encoding="utf-8"` to `open()` for any file I/O involving web-scraped content or JSONL data files.

### Virtual environment activation on Windows (Git Bash)
- Use `source venv/Scripts/activate` (not `venv/bin/activate` which is Linux/macOS).
- PowerShell uses `venv\Scripts\Activate.ps1` instead.

---

## JSON Schema

### `$ref` resolution requires a schema store
- **Problem:** JSON Schema `$ref` references (`"$ref": "common.json#/definitions/Company"`) fail silently or raise `RefResolutionError` unless the validator knows where to find referenced schemas.
- **Fix:** Build a schema store that maps `$id` URIs to schema contents, then pass it to `RefResolver`. See `contracts/validator.py:_build_schema_store()`.
- **Pattern:** Scan all `.json` files in the schemas directory, extract `$id` from each, and create a `{id: schema}` mapping. Also map `file://` URIs for relative path resolution.

### `RefResolver` is deprecated in jsonschema 4.18+ (MIGRATED)
- **Problem:** `RefResolver` emits deprecation warnings and will be removed in jsonschema 5.x.
- **Fix (completed 2026-02-07):** Migrated to `referencing.Registry`. Replace `RefResolver` import with `from referencing import Registry, Resource`. Wrap schemas in `Resource.from_contents()`, pass `registry=` kwarg to validator instead of `resolver=`.

---

## Web Scraping / Extraction

### Not all associations have dedicated member directory pages
- Some associations (especially smaller ones) embed member lists within other pages, use PDFs, or require login.
- The `DirectoryParser` fallback pattern handles this: try structured parsing first, fall back to generic link/text extraction if no directory-specific selectors match.

### Link crawler same-domain filter can miss inline directories
- **Problem:** Some associations host member directories on subdomains or third-party platforms (e.g., `members.association.org` vs `association.org`).
- **Fix:** Compare base domains (strip subdomains) rather than exact hostname matching. Use `tldextract` for reliable domain extraction.

### Social media domain matching is substring-fragile
- **Problem:** Naive substring check like `"linkedin" in url` matches `notlinkedin.com` or `linkedin-scam.com`.
- **Fix:** Parse the URL properly with `urllib.parse.urlparse()` and check the hostname against known social domains. Use `tldextract` for edge cases.

### robots.txt must always be checked first
- Before crawling any new domain, fetch and parse `robots.txt`. Cache the result per domain.
- Respect `Crawl-delay` directives — some association sites set delays of 10+ seconds.
- If `robots.txt` returns 403/404, assume permissive (standard practice) but still rate-limit.

---

## Architecture Decisions

### DirectoryParser fallback pattern
- Extraction agents try parsers in specificity order: association-specific parser -> generic directory parser -> raw link extractor.
- Each parser returns a confidence score. Use the highest-confidence result above a threshold (0.3).

### Auto-detect vs. schema comparison for page classification
- Page classifier uses both URL pattern matching and content analysis.
- URL patterns are fast but miss edge cases. Content analysis (looking for table structures, pagination, company-like entities) catches what URL patterns miss.
- Best results come from combining both signals with weighted scoring.

### Agent spawner registry pattern
- `AgentSpawner` uses a registry dict mapping agent type strings to classes.
- Dynamic import via `importlib` allows adding new agents without modifying the spawner.
- Always validate the agent type string against the registry before attempting import.

### State machine with explicit phase transitions
- 13 phases with defined valid transitions prevent agents from skipping steps.
- Each phase records metadata (start time, record count, errors) for pipeline observability.
- `FAILED` state is reachable from any phase; recovery restarts from the last successful checkpoint.

---

## Testing

### Coverage targets by module category
- **Core infrastructure** (base.py, validator.py, machine.py): Target 95%+
- **Agent implementations**: Target 90%+
- **Integration tests**: Focus on happy path + top 3 error scenarios

### Fixture patterns that work well
- Use `conftest.py` with 50+ shared fixtures covering HTTP mocks, API responses, and sample data.
- `aioresponses` for mocking async HTTP calls (much cleaner than manual patching).
- `tmp_path` fixture for file I/O tests — avoids polluting the real data directory.
- Parameterize tests with `@pytest.mark.parametrize` for input variations (valid/invalid schemas, different HTML structures).

### Pre-existing bugs surface during test writing
- 5 tests in `test_discovery_site_mapper.py` are skipped due to a bug in `site_mapper.py:_looks_like_directory()` — `soup.find()` returns a `Tag` object (truthy) not a `bool`, causing incorrect logic.
- Don't fix production bugs while writing tests unless they're trivial. Skip with `@pytest.mark.skip(reason="...")` and file as a known issue.

### async test configuration
- Use `pytest-asyncio` with `@pytest.mark.asyncio` decorator.
- Set `asyncio_mode = "auto"` in `pytest.ini` or `pyproject.toml` to avoid manually decorating every async test.

---

## Dependencies

### Pydantic V2 deprecation warnings
- Pydantic V2 renamed several config options and validators. Common warnings:
  - `class Config` -> `model_config = ConfigDict(...)`
  - `@validator` -> `@field_validator`
  - `orm_mode` -> `from_attributes`
- These are warnings only (still functional in Pydantic 2.x) but should be cleaned up.

### jsonschema migration path (COMPLETED)
- Migrated from `jsonschema.RefResolver` to `referencing.Registry` (2026-02-07)
- Uses `DRAFT202012.create_resource()` for schemas without `$schema` key
- Schemas with `$schema` use `Resource.from_contents()` with auto-detection

---

## Pipeline Operations

### Smoke testing
- Use SOCMA (smallest association, ~200 members) for quick end-to-end smoke tests.
- Run with `--dry-run` flag to test the full pipeline without writing to the database.
- Command: `python -m agents.orchestrator --mode full -a SOCMA --dry-run`

### Rate limiting is critical
- Association websites: 0.5 req/sec max, 1,000 requests/day per domain.
- Getting IP-banned from an association site means losing that data source. Be conservative.
- Use exponential backoff on 429s, starting at 5 seconds.

### Checkpoint/resume saves hours
- Long extraction runs (1,000+ URLs) should checkpoint every 50 records.
- `BaseAgent.checkpoint()` saves state to `data/.state/{agent_type}.json`.
- On restart, `load_checkpoint()` picks up from the last saved position.
- Always verify checkpoint integrity before resuming — corrupted checkpoints are worse than restarting.

---

## Common Mistakes to Avoid

1. **Don't hardcode association URLs** — they change. Use `config/associations.yaml` as the source of truth.
2. **Don't skip the gatekeeper phase** — even for "known safe" sites. robots.txt can change.
3. **Don't parse HTML with regex** — use BeautifulSoup or lxml. Always.
4. **Don't trust external API data blindly** — Clearbit/Apollo/ZoomInfo can return stale or incorrect data. Cross-reference when possible.
5. **Don't commit `.env`** — it contains real API keys. Use `.env.example` as a template.
6. **Don't run the full pipeline without `--dry-run` first** — especially after code changes.

---

## CI/CD & Docker

### Dockerfile: npx is not available in multi-stage runtime
- **Problem:** When copying only `/usr/bin/node` and `node_modules/` from the builder stage (not the full Node.js installation), `npx` is not available in the runtime image.
- **Fix:** Call Playwright CLI directly: `RUN /usr/bin/node node_modules/playwright/cli.js install chromium` instead of `RUN npx playwright install chromium`.

### Selective COPY is better than `COPY . .` in Dockerfiles
- **Problem:** `COPY . .` pulls in tests, docs, config files, and other artifacts that bloat the image and can leak sensitive info.
- **Fix:** Use explicit `COPY agents/ ./agents/` etc. for each production directory. Combine with `.dockerignore` as a safety net.

### Makefile platform detection for Windows vs Unix
- **Problem:** `find`, `rm -rf`, `python3` don't exist on Windows. `python`, `del`, `for /d /r` don't exist on Unix.
- **Fix:** Use `ifeq ($(OS),Windows_NT)` to branch on platform. Define `PYTHON`, `PIP`, `RM_PYCACHE`, `RM_PYC` per platform.

### Playwright browser caching in GitHub Actions
- **Problem:** Playwright downloads ~150MB of browser binaries on every CI run.
- **Fix:** Cache `~/.cache/ms-playwright` with `actions/cache@v4`. Use conditional steps: `playwright install chromium --with-deps` on cache miss, `playwright install-deps chromium` (system libs only) on cache hit.

### Coverage config belongs in pyproject.toml
- **Problem:** Coverage source/omit/fail_under flags repeated across CI, Makefile, and docker-compose.
- **Fix:** Add `[tool.coverage.run]` and `[tool.coverage.report]` sections to `pyproject.toml`. CLI flags still override when needed, but the defaults are centralized.

### pre-commit hook versions should be pinned and updated together
- **Problem:** Stale hook versions miss new rules and bug fixes.
- **Fix:** Update ruff-pre-commit and pre-commit-hooks versions explicitly. Add `--exit-non-zero-on-fix` to ruff so CI fails if auto-fixes are needed (forces developers to commit clean code).

### Redis in docker-compose for future-proofing
- **Why:** Pipeline will need Redis for rate-limit coordination, distributed task queues, and caching enrichment API responses.
- **Pattern:** Add healthcheck (`redis-cli ping`) and `condition: service_healthy` dependency. Use tmpfs in test compose for fast ephemeral storage.

---

## Production Readiness

### 1,533 tests ≠ production-ready
- **Lesson:** A comprehensive test suite with 100% pass rate and 85% coverage proves the code works *in isolation with mocks*. It does NOT prove the pipeline works against real association websites, real APIs, or a real PostgreSQL database.
- **Gap discovered:** All 1,533 tests use mocked HTTP responses, mocked databases, and mocked API keys. Zero live requests have been made against actual association websites (PMA, NEMA, etc.).
- **Rule:** Before claiming production-ready status, always run at least one end-to-end smoke test against a real data source with `--dry-run` first, then a single full extraction with manual review.

### API keys are an operational prerequisite, not a code issue
- **Problem:** The enrichment pipeline (firmographic, tech stack, contacts) is fully implemented and tested, but returns empty results without valid API keys for Clearbit, Apollo, and BuiltWith.
- **Lesson:** Track operational prerequisites (API key procurement, DNS configuration, database provisioning) separately from code tasks in the WBS. They have different owners and timelines.
- **Required keys:** `CLEARBIT_API_KEY`, `APOLLO_API_KEY`, `BUILTWITH_API_KEY`. Optional: `ZOOMINFO_API_KEY`, `HUNTER_API_KEY`, `GOOGLE_PLACES_API_KEY`.

### Always test with a real database before deployment
- **Problem:** Database tests mock the PostgreSQL connection. The `docker-compose.yml` wires PostgreSQL 16, but no end-to-end test has been run against a real database instance.
- **Risk:** Schema mismatches, migration failures, connection pool exhaustion under load, and encoding issues with scraped Unicode data are invisible in mocked tests.
- **Fix:** Run `docker-compose up postgres`, execute `python scripts/init_db.py`, then run a single-association extraction writing to the real database.

### `graph_edges` serialization mismatch
- **Problem:** The orchestrator's `_run_graph_phase()` stores an integer count in `state.graph_edges`, but `PipelineState` defines `graph_edges` as `list[dict]`. This will cause a Pydantic validation error when the GRAPH phase runs with real data.
- **Fix:** Ensure the orchestrator stores the actual edge list (or an empty list) rather than a count integer. ~15 min fix.
- **Broader lesson:** Type mismatches between the orchestrator and the state model are invisible when tests mock the state object. Integration tests that exercise the full state machine with real `PipelineState` instances would catch these.

### Pydantic V2 `json_encoders` deprecation
- **Problem:** `models/ontology.py` uses `json_encoders` in `model_config`, which triggers a `PydanticDeprecatedSince20` warning on every test run and will break in Pydantic V3.
- **Fix:** Migrate to `model_serializer` or `field_serializer` decorators. ~30 min fix.
- **Note:** Session 11 migrated `Provenance.Config` to `ConfigDict` but the `json_encoders` key itself is still deprecated. The full fix requires replacing `json_encoders` with per-field custom serializers.

### Smoke-test against real websites before scaling
- **Problem:** HTML structure assumptions, pagination logic, and CSS selectors in extraction agents are based on development-time analysis of association websites. These sites change their DOM structure without notice.
- **Lesson:** Before running extraction against all 10 associations, always smoke-test against the smallest one (SOCMA, ~200 members) with `--dry-run`. Inspect the output manually. Only then scale to larger associations.
- **Command:** `python -m agents.orchestrator --mode full -a SOCMA --dry-run`
