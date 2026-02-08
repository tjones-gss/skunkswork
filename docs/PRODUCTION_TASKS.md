# Production Tasks — NAM Intelligence Pipeline

> **Created:** 2026-02-08 (Session 16)
> **Status:** 19/21 WBS tasks complete (90%) — Grade: A-
> **Purpose:** Delegated task breakdown for advancing to production readiness

---

## Tier 1: Production Prerequisites (~8h)

These are **blockers** — nothing else should start until these are resolved.

---

### T1-01: Fix `graph_edges` Serialization Bug

| Field | Value |
|-------|-------|
| **Effort** | 15 minutes |
| **Files** | `agents/orchestrator.py` (line 683) |
| **Developer** | Any — quick fix |
| **Dependencies** | None |

**Problem:** The orchestrator stores an integer count in `state.graph_edges`, but `PipelineState.graph_edges` is typed as `list[dict]`. This causes a `PydanticSerializationUnexpectedValue` warning and will fail validation on a real GRAPH phase run.

**Current code (line 683):**
```python
self.state.graph_edges = result.get("edges_created", 0)
```

**Fix:**
```python
self.state.graph_edges = result.get("edges", [])
```

**Acceptance Criteria:**
- [ ] `state.graph_edges` stores a `list[dict]`, not an integer
- [ ] Edge count is tracked separately (e.g., `len(self.state.graph_edges)`)
- [ ] All existing tests pass: `pytest tests/ -v --tb=short`
- [ ] No `PydanticSerializationUnexpectedValue` warning in test output

---

### T1-02: Fix Pydantic V2 `json_encoders` Deprecation

| Field | Value |
|-------|-------|
| **Effort** | 30 minutes |
| **Files** | `models/ontology.py` (line 133) |
| **Developer** | Any — Pydantic migration |
| **Dependencies** | None |

**Problem:** The `Provenance` class uses the deprecated `json_encoders` in `model_config`. This triggers a `PydanticDeprecatedSince20` warning on every test run and will break in Pydantic V3.

**Current code (line 133):**
```python
model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})
```

**Fix:** Replace with `field_serializer` decorator:
```python
from pydantic import field_serializer

class Provenance(BaseModel):
    # ... existing fields ...

    @field_serializer('extracted_at')
    @classmethod
    def serialize_datetime(cls, v: datetime) -> str:
        return v.isoformat()
```

**Acceptance Criteria:**
- [ ] No `PydanticDeprecatedSince20` warning in test output
- [ ] `Provenance.model_dump_json()` still serializes `extracted_at` as ISO string
- [ ] All existing tests pass: `pytest tests/ -v --tb=short`
- [ ] `model_config` line removed or updated to remove `json_encoders` key

---

### T1-03: Procure API Keys

| Field | Value |
|-------|-------|
| **Effort** | 2 hours |
| **Files** | `.env` (configure), `.env.example` (verify) |
| **Developer** | Ops / Team Lead — requires account sign-ups |
| **Dependencies** | None |

**Required API Keys:**

| Service | Env Variable | Sign-up URL | Purpose |
|---------|-------------|-------------|---------|
| Clearbit | `CLEARBIT_API_KEY` | https://clearbit.com | Company enrichment (firmographics) |
| Apollo | `APOLLO_API_KEY` | https://www.apollo.io | Contact finder + firmographics |
| BuiltWith | `BUILTWITH_API_KEY` | https://builtwith.com | Tech stack detection |

**Optional API Keys:**

| Service | Env Variable | Purpose |
|---------|-------------|---------|
| ZoomInfo | `ZOOMINFO_API_KEY` | Sales intelligence (fallback) |
| Hunter | `HUNTER_API_KEY` | Email verification |
| Google Places | `GOOGLE_PLACES_API_KEY` | Address validation |

**Acceptance Criteria:**
- [ ] `.env` file contains valid keys for Clearbit, Apollo, and BuiltWith
- [ ] `python -c "from middleware.secrets import SecretsManager; sm = SecretsManager(); print(sm.get('CLEARBIT_API_KEY')[:8])"` returns a partial key
- [ ] Rate limits configured in `config/agents.yaml` match API plan tier

---

### T1-04: PMA Smoke-Test (First Live Extraction)

| Field | Value |
|-------|-------|
| **Effort** | 4 hours |
| **Files** | No code changes — operational |
| **Developer** | Senior Dev — first live run requires monitoring |
| **Dependencies** | T1-01, T1-02, T1-03 |

**Execution Steps:**

```bash
# Step 1: Dry run (no HTTP requests)
python -m agents.orchestrator --mode extract -a PMA --dry-run

# Step 2: Single page test (limit to 1 URL)
python -m agents.orchestrator --mode extract -a PMA --max-pages 1

# Step 3: Small batch (10 pages)
python -m agents.orchestrator --mode extract -a PMA --max-pages 10

# Step 4: Full extraction
python -m agents.orchestrator --mode extract -a PMA
```

**Monitor during each step:**
- `data/raw/PMA/urls.jsonl` — discovered URLs
- `data/raw/PMA/records.jsonl` — extracted company records
- `logs/` — structured JSON logs for errors
- Rate limiting compliance (0.5 req/sec, max 1,000/day)

**Acceptance Criteria:**
- [ ] Dry run completes without errors
- [ ] Single page extraction produces valid company records
- [ ] Full PMA extraction yields ≥50 company records
- [ ] No 429 (rate limit) responses in logs
- [ ] robots.txt respected for pma.org domain

---

### T1-05: PostgreSQL Integration Test

| Field | Value |
|-------|-------|
| **Effort** | 2 hours |
| **Files** | No code changes — infrastructure |
| **Developer** | Ops / Dev with Docker |
| **Dependencies** | T1-04 (need extracted data to load) |

**Execution Steps:**

```bash
# Step 1: Start PostgreSQL
docker-compose up -d postgres

# Step 2: Initialize schema
python scripts/init_db.py

# Step 3: Load extracted PMA data into database
python -m agents.orchestrator --mode full -a PMA

# Step 4: Verify data in database
psql $DATABASE_URL -c "SELECT COUNT(*) FROM companies;"
psql $DATABASE_URL -c "SELECT company_name, domain, state FROM companies LIMIT 10;"
```

**Acceptance Criteria:**
- [ ] `docker-compose up -d postgres` starts without errors
- [ ] `python scripts/init_db.py` creates all tables (companies, contacts, extraction_jobs, etc.)
- [ ] Extracted PMA records load successfully into `companies` table
- [ ] Query returns expected record count matching `data/raw/PMA/records.jsonl`

---

## Tier 2: Data Operations (~16h, requires Tier 1 API keys)

Once API keys are configured and PMA smoke-test passes, these are the value-generating activities.

---

### T2-01: Firmographic Enrichment

| Field | Value |
|-------|-------|
| **Effort** | 4 hours |
| **Files** | `agents/enrichment/firmographic.py` — no code changes, operational run |
| **Developer** | Dev — monitor API responses |
| **Dependencies** | T1-03 (Clearbit/Apollo keys), T1-04 (PMA records) |

**Command:**
```bash
python -m agents.orchestrator --mode enrich --enrichment firmographic
```

**What it does:**
- Queries Clearbit API for each company's domain → employee count, revenue, industry
- Falls back to Apollo API if Clearbit misses
- Falls back to ZoomInfo if Apollo misses
- Last resort: scrapes company website About page

**Provider chain:** `clearbit` → `apollo` → `zoominfo` → `website` (config: `config/agents.yaml` lines 114-121)

**Acceptance Criteria:**
- [ ] Enrichment completes without API errors
- [ ] ≥50% of PMA records have `employee_count_min` populated
- [ ] Output in `data/processed/enriched.jsonl` contains firmographic fields
- [ ] Rate limits respected: Clearbit ≤10 req/sec, Apollo ≤0.8 req/sec

---

### T2-02: Tech Stack Detection

| Field | Value |
|-------|-------|
| **Effort** | 4 hours |
| **Files** | `agents/enrichment/tech_stack.py` — no code changes, operational run |
| **Developer** | Dev — monitor API responses |
| **Dependencies** | T1-03 (BuiltWith key), T2-01 (enriched records) |

**Command:**
```bash
python -m agents.orchestrator --mode enrich --enrichment techstack
```

**What it does:**
- Queries BuiltWith API for each company's domain → technology stack
- Falls back to website fingerprinting (scan for ERP/CRM keywords in page source)
- Falls back to job posting analysis (scan for ERP mentions in job listings)

**Detection targets:** SAP, Oracle, Epicor, Infor, Microsoft Dynamics, NetSuite, SYSPRO, Plex, and 20+ ERP/CRM systems defined in `TechStackAgent.ERP_KEYWORDS` and `TechStackAgent.CRM_KEYWORDS`.

**Acceptance Criteria:**
- [ ] Detection completes without API errors
- [ ] ≥20% of records have `erp_system` populated (industry benchmark: 30-40% detectable)
- [ ] `tech_stack` array populated with technology names
- [ ] Rate limits respected: BuiltWith ≤5 req/sec

---

### T2-03: Contact Finder

| Field | Value |
|-------|-------|
| **Effort** | 4 hours |
| **Files** | `agents/enrichment/contact_finder.py` — no code changes, operational run |
| **Developer** | Dev — monitor API responses, verify contact quality |
| **Dependencies** | T1-03 (Apollo key), T2-01 (enriched records) |

**Command:**
```bash
python -m agents.orchestrator --mode enrich --enrichment contacts
```

**What it does:**
- Queries Apollo API for decision-makers at each company
- Targets 17 title patterns including CIO, VP IT, IT Director, COO, CFO, CEO, ERP Manager, Plant Manager
- Falls back to ZoomInfo if Apollo misses
- Falls back to scraping company team/about pages

**Target titles** (from `config/agents.yaml` lines 137-155):
CIO, VP IT, IT Director, Chief Information Officer, VP Information Technology, Director of IT, ERP Manager, COO, VP Operations, Chief Operating Officer, CFO, Controller, CEO, President, Owner, Plant Manager, VP Manufacturing

**Acceptance Criteria:**
- [ ] Contact search completes without API errors
- [ ] ≥40% of records have at least 1 contact
- [ ] Contacts include name, title, and email at minimum
- [ ] Max 5 contacts per company (configured: `max_contacts_per_company: 5`)

---

### T2-04: Multi-Association Extraction (NEMA, SOCMA, AGMA)

| Field | Value |
|-------|-------|
| **Effort** | 4 hours |
| **Files** | No code changes — operational run |
| **Developer** | Dev — monitor each association's extraction |
| **Dependencies** | T1-04 (PMA smoke-test validates pipeline) |

**Command:**
```bash
python -m agents.orchestrator --mode extract -a NEMA -a SOCMA -a AGMA
```

**Association details** (from `config/associations.yaml`):
- **NEMA** — National Electrical Manufacturers Association (nema.org)
- **SOCMA** — Society of Chemical Manufacturers & Affiliates (socma.org)
- **AGMA** — American Gear Manufacturers Association (agma.org)

**Acceptance Criteria:**
- [ ] Each association produces ≥20 company records
- [ ] Output files: `data/raw/{NEMA,SOCMA,AGMA}/records.jsonl`
- [ ] robots.txt respected for each domain
- [ ] No 429 or 403 errors in logs
- [ ] Total records across all associations ≥100


---

## Tier 3: Remaining WBS Tasks (~56h)

These are **not blockers** for initial production use. They become important at scale.

---

### P4-T03: Incremental Extraction (Delta Updates)

| Field | Value |
|-------|-------|
| **Effort** | 16 hours |
| **WBS ID** | P4-T03 |
| **Files** | `agents/enrichment/firmographic.py`, `db/`, `models/ontology.py` |
| **Developer** | Senior Dev — requires database schema changes |
| **Dependencies** | P3-T02 ✅ (partial-phase resume), P2-T04 ✅ (API key check) |

**Description:** Implement delta extraction so re-running extraction on an already-processed association only fetches pages that have changed since the last run. Avoids re-scraping unchanged pages, reduces API costs, and speeds up re-extraction cycles.

**Implementation approach:**
1. Store `last_extracted_at` timestamp per URL in `url_queue` table
2. Compare page `Last-Modified` / `ETag` headers against stored values
3. Skip unchanged pages during EXTRACTION phase
4. Track extraction delta stats (new, changed, unchanged, deleted)

**Acceptance Criteria:**
- [ ] Re-running `--mode extract -a PMA` only fetches changed pages
- [ ] `url_queue` table stores `last_modified` and `etag` per URL
- [ ] Delta stats logged: `{new: 5, changed: 3, unchanged: 192, deleted: 0}`
- [ ] `--force-full` flag available to bypass delta logic
- [ ] Tests: ≥10 unit tests covering delta detection and bypass

---

### P4-T05: Admin Dashboard with Pipeline Status

| Field | Value |
|-------|-------|
| **Effort** | 20 hours |
| **WBS ID** | P4-T05 |
| **Files** | New `dashboard/` directory |
| **Developer** | Full-stack Dev — web UI required |
| **Dependencies** | P2-T01 ✅ (Prometheus metrics), P2-T05 ✅ (health summary), P3-T02 ✅ (partial resume) |

**Description:** Build a lightweight web dashboard showing pipeline status, extraction progress, data quality metrics, and agent health. Uses existing Prometheus metrics endpoint.

**Suggested stack:**
- FastAPI or Flask for backend
- HTMX or simple React for frontend
- Charts.js or similar for visualizations
- Reads from PostgreSQL and Prometheus metrics

**Dashboard pages:**
1. **Overview** — Pipeline status, WBS completion, record counts by association
2. **Extraction** — Per-association progress, pages crawled vs remaining, error rates
3. **Enrichment** — Provider hit rates, API quota usage, coverage percentages
4. **Data Quality** — Score distribution, grade breakdown, field completeness heat map
5. **Agents** — Agent health, circuit breaker status, rate limit headroom

**Acceptance Criteria:**
- [ ] Dashboard accessible at `http://localhost:8080`
- [ ] Shows real-time pipeline phase and progress
- [ ] Displays per-association extraction stats
- [ ] Shows enrichment provider hit rates
- [ ] Data quality score distribution chart
- [ ] Tests: ≥15 unit tests for API endpoints

---

### P4-T06: Horizontal Scaling via Celery Task Queue

| Field | Value |
|-------|-------|
| **Effort** | 20 hours |
| **WBS ID** | P4-T06 |
| **Files** | `agents/base.py`, new `workers/` directory |
| **Developer** | Senior Dev — distributed systems experience |
| **Dependencies** | P3-T01 ✅ (load tests), P3-T04 ✅ (memory profiling), P4-T01 ✅ (Vault) |

**Description:** Enable horizontal scaling by wrapping agents in Celery tasks so multiple associations can be processed concurrently across worker processes. Required when processing 10+ associations simultaneously.

**Implementation approach:**
1. Add `CeleryAgentMixin` to `BaseAgent` in `agents/base.py`
2. Create `workers/celery_app.py` with task definitions
3. Create `workers/tasks.py` wrapping each agent type as a Celery task
4. Add Redis/RabbitMQ broker configuration
5. Implement result backend for task status tracking

**Commands:**
```bash
# Start Celery worker
celery -A workers.celery_app worker --loglevel=info --concurrency=4

# Start Celery beat (scheduled tasks)
celery -A workers.celery_app beat --loglevel=info

# Submit extraction job
python -m agents.orchestrator --mode extract-all --backend celery
```

**Acceptance Criteria:**
- [ ] `CeleryAgentMixin` in `agents/base.py` wraps agent `run()` as Celery task
- [ ] Concurrent extraction of 3+ associations across worker processes
- [ ] Task status visible via Celery Flower or dashboard
- [ ] Graceful shutdown preserves checkpoints
- [ ] Tests: ≥10 unit tests for task serialization and result handling

---

## Tier 4: Readiness Checklist Items

Two production readiness checklist items remain at ⚠️ status. These are acceptable for initial deployment but should be addressed before full production.

---

### T4-01: Monitoring Dashboards

| Field | Value |
|-------|-------|
| **Effort** | 4 hours |
| **Files** | `docker-compose.yml` (add Grafana), new `monitoring/` config files |
| **Developer** | Ops |
| **Dependencies** | P2-T01 ✅ (Prometheus metrics already wired) |

**Description:** Configure Grafana dashboards to visualize the Prometheus metrics already exposed by the pipeline. Metrics are wired but no dashboards exist.

**Acceptance Criteria:**
- [ ] Grafana accessible at `http://localhost:3000`
- [ ] Dashboard showing agent execution times, error rates, and throughput
- [ ] Alert rules for extraction failure rate >10% and enrichment API errors

---

### T4-02: HTML Sanitization

| Field | Value |
|-------|-------|
| **Effort** | 4 hours |
| **Files** | `agents/extraction/html_parser.py`, `utils/sanitize.py` (new) |
| **Developer** | Dev |
| **Dependencies** | None |

**Description:** Sanitize scraped HTML content before storing or rendering. Only matters if data is displayed in a web UI (P4-T05 dashboard). Low risk if pipeline output is consumed only by downstream processing.

**Acceptance Criteria:**
- [ ] All HTML content sanitized using `bleach` or equivalent before storage
- [ ] Script tags, event handlers, and iframes removed
- [ ] Tests: ≥5 unit tests for XSS attack vector sanitization

---

## Quick Reference Checklist

Copy-paste into GitHub Issues or project management tool. Check off as completed.

### Tier 1: Production Prerequisites
- [ ] **T1-01** Fix `graph_edges` serialization bug (`agents/orchestrator.py` line 683) — 15min
- [ ] **T1-02** Fix Pydantic V2 `json_encoders` deprecation (`models/ontology.py` line 133) — 30min
- [ ] **T1-03** Procure API keys: Clearbit, Apollo, BuiltWith — 2h
- [ ] **T1-04** PMA smoke-test: dry-run → single page → full extraction — 4h
- [ ] **T1-05** PostgreSQL integration test: docker-compose + init_db.py — 2h

### Tier 2: Data Operations
- [ ] **T2-01** Run firmographic enrichment on PMA companies — 4h
- [ ] **T2-02** Run tech stack detection (BuiltWith) — 4h
- [ ] **T2-03** Run contact finder (Apollo/ZoomInfo) — 4h
- [ ] **T2-04** Live extraction: NEMA, SOCMA, AGMA — 4h

### Tier 3: Remaining WBS Tasks
- [ ] **P4-T03** Incremental extraction (delta updates) — 16h
- [ ] **P4-T05** Admin dashboard with pipeline status — 20h
- [ ] **P4-T06** Horizontal scaling via Celery task queue — 20h

### Tier 4: Readiness Items
- [ ] **T4-01** Monitoring dashboards (Grafana) — 4h
- [ ] **T4-02** HTML sanitization — 4h

---

**Total remaining effort: ~84 hours**
- Tier 1: 8h (blocks everything)
- Tier 2: 16h (generates value)
- Tier 3: 56h (scales the pipeline)
- Tier 4: 8h (hardens for production)

**Timeline:**
| Milestone | Effort | Result |
|-----------|--------|--------|
| First live extraction | ~12h (Tier 1) | Validated PMA company records |
| Controlled production | ~28h (Tier 1+2) | 4 associations, enriched + contacts |
| Full production | ~84h (all tiers) | 10 associations, 10K+ companies, dashboard |
