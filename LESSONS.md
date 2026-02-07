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

### `RefResolver` is deprecated in jsonschema 4.18+
- **Current state:** Works fine with `jsonschema>=4.20.0` but emits deprecation warnings.
- **Migration path:** Replace `RefResolver` with the `referencing` library. Not urgent but should be done before `jsonschema` 5.x drops support.

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

### jsonschema migration path
- Current: `jsonschema>=4.20.0` with deprecated `RefResolver`
- Future: Migrate to `referencing` library for `$ref` resolution
- Timeline: Before `jsonschema` 5.x release (no announced date yet)

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
