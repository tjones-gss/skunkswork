# NAM Intelligence Pipeline — Operational Runbook

> **Version:** 1.0 | **Last updated:** 2026-02-08 | **Owner:** GSS Marketing Skunkswork

---

## Table of Contents

1. [Quick Reference](#1-quick-reference)
2. [Deployment](#2-deployment)
3. [Running the Pipeline](#3-running-the-pipeline)
4. [Monitoring & Observability](#4-monitoring--observability)
5. [Troubleshooting](#5-troubleshooting)
6. [Rate Limit Management](#6-rate-limit-management)
7. [API Key Rotation](#7-api-key-rotation)
8. [Database Operations](#8-database-operations)
9. [Backup & Recovery](#9-backup--recovery)
10. [Maintenance Procedures](#10-maintenance-procedures)

---

## 1. Quick Reference

### Key Commands

| Action | Command |
|--------|---------|
| Run full pipeline (single) | `python -m agents.orchestrator --mode full -a PMA` |
| Run full pipeline (multi) | `python -m agents.orchestrator --mode full -a PMA -a NEMA -a AGMA` |
| Extract all associations | `python -m agents.orchestrator --mode extract-all` |
| Dry run (no writes) | `python -m agents.orchestrator --mode full --dry-run -a PMA` |
| Resume failed job | `python -m agents.orchestrator --mode full --resume <job-id>` |
| Persist to PostgreSQL | `python -m agents.orchestrator --mode full -a PMA --persist-db` |
| Run tests | `pytest tests/ -v` |
| Init database | `python scripts/init_db.py` |
| Reset database | `python scripts/init_db.py --drop --force` |
| Health check | `python scripts/healthcheck.py` |
| Docker start | `docker compose up -d` |
| Docker stop | `docker compose down` |

### Key File Locations

| Item | Path |
|------|------|
| Pipeline state | `data/.state/<job-id>/state.json` |
| Checkpoints | `data/.state/<job-id>/checkpoint_*.json` |
| Health check output | `data/.state/<job-id>/health_check.json` |
| Raw extraction data | `data/raw/<association>/records.jsonl` |
| Enriched data | `data/processed/enriched.jsonl` |
| Validated output | `data/validated/<timestamp>/companies.jsonl` |
| Export files | `data/exports/` |
| Dead letter queue | `data/dead_letter/` |
| Monitoring baselines | `data/monitoring/baselines/` |
| Monitoring reports | `data/monitoring/reports/` |
| Logs | `logs/` |

### Environment Files

| File | Purpose |
|------|---------|
| `.env` | Active configuration (never commit) |
| `.env.example` | Template with all variables documented |
| `config/agents.yaml` | Agent-level config (timeouts, batch sizes, rate limits) |
| `config/associations.yaml` | Association URLs and metadata |

---

## 2. Deployment

### 2.1 Local Development Setup

```bash
# 1. Clone and enter project
cd nam-intelligence-pipeline

# 2. Create virtual environment
python -m venv venv
venv\Scripts\activate           # Windows
# source venv/bin/activate      # Linux/Mac

# 3. Install dependencies
pip install -r requirements.txt
npm install
npx playwright install chromium

# 4. Configure environment
cp .env.example .env
# Edit .env with your API keys and database URL

# 5. Initialize database
python scripts/init_db.py

# 6. Verify setup
python scripts/healthcheck.py   # Should exit 0
pytest tests/ -v                # Should pass 1,532+ tests
```

### 2.2 Docker Deployment

```bash
# Build image
docker build -t nam-pipeline .

# Start all services (app + PostgreSQL + Redis)
docker compose up -d

# Verify health
docker compose ps               # All services should show "healthy"
docker compose exec app python scripts/healthcheck.py

# Start with pgAdmin (optional admin UI on port 5050)
docker compose --profile admin up -d

# View logs
docker compose logs -f app
```

### 2.3 Docker Compose Services

| Service | Port | Purpose |
|---------|------|---------|
| `app` | — | Pipeline application |
| `postgres` | 5432 | PostgreSQL 16 database |
| `redis` | 6379 | Redis 7 (caching/queues) |
| `pgadmin` | 5050 | Database admin UI (optional, `--profile admin`) |

### 2.4 Required API Keys

| Key | Required | Provider | Used By |
|-----|----------|----------|---------|
| `CLEARBIT_API_KEY` | **Yes** | [clearbit.com](https://clearbit.com) | Firmographic enrichment |
| `APOLLO_API_KEY` | **Yes** | [apollo.io](https://apollo.io) | Sales intel, contacts |
| `BUILTWITH_API_KEY` | **Yes** | [builtwith.com](https://builtwith.com) | Tech stack detection |
| `ZOOMINFO_API_KEY` | Optional | [zoominfo.com](https://zoominfo.com) | B2B intelligence |
| `HUNTER_API_KEY` | Optional | [hunter.io](https://hunter.io) | Email verification |
| `GOOGLE_PLACES_API_KEY` | Optional | [Google Cloud](https://console.cloud.google.com) | Address verification |
| `LINKEDIN_API_KEY` | Optional | [Proxycurl](https://nubela.co/proxycurl) | LinkedIn validation |

**Note:** The pipeline will run without optional keys but with reduced enrichment quality. The INIT phase health check logs which keys are present/missing.

---

## 3. Running the Pipeline

### 3.1 Pipeline Modes

| Mode | Description | Example |
|------|-------------|---------|
| `full` | Complete 11-phase pipeline | `--mode full -a PMA` |
| `extract` | Discovery + extraction only | `--mode extract -a PMA` |

| # | Phase | Agent(s) | What It Does |
|----|-------|----------|--------------|
| 1 | INIT | — | Load config, health check, seed URL queue |
| 2 | GATEKEEPER | `access_gatekeeper` | Check robots.txt, ToS, auth requirements |
| 3 | DISCOVERY | `site_mapper`, `link_crawler` | Map site structure, crawl member directories |
| 4 | CLASSIFICATION | `page_classifier` | Route pages by type (member, event, other) |
| 5 | EXTRACTION | `html_parser`, `event_extractor`, `event_participant_extractor` | Parse company/event data from HTML |
| 6 | ENRICHMENT | `firmographic`, `tech_stack`, `contact_finder` | Add firmographics, tech stack, contacts |
| 7 | VALIDATION | `dedupe`, `crossref`, `scorer` | Deduplicate, verify, score records |
| 8 | RESOLUTION | `entity_resolver` | Merge duplicate entities into canonical records |
| 9 | GRAPH | `competitor_signal_miner`, `relationship_graph_builder` | Build competitive intelligence graph |
| 10 | EXPORT | `export_activation` | Generate CSV/JSON/CRM export files |
| 11 | MONITOR | `source_monitor` | Baseline tracking for DOM drift detection |

### 3.3 Resume After Failure

The pipeline checkpoints its state after each phase and periodically within phases. To resume:

```bash
# 1. Find the failed job ID from logs or state directory
ls data/.state/

# 2. Resume from where it left off
python -m agents.orchestrator --mode full --resume <job-id>
```

**Intra-phase resume** is also supported. Each phase tracks progress in `state.phase_progress`:
- GATEKEEPER: Tracks `checked_domains` — skips already-checked domains
- DISCOVERY/EXTRACTION: Tracks items processed via `visited_urls`
- CLASSIFICATION: Tracks `classified_urls` — skips already-classified pages
- ENRICHMENT: Tracks `completed_steps` (firmographic → tech_stack → contact_finder)
- VALIDATION: Tracks `completed_steps` (dedupe → crossref → scorer)
- RESOLUTION: Tracks `resolved` boolean flag
- GRAPH: Tracks `mined_company_ids` + `graph_built` flag
- EXPORT: Tracks `completed_exports` (companies → events → summary)

### 3.4 CLI Options

```
python -m agents.orchestrator [OPTIONS]

Options:
  --mode          Pipeline mode (full|extract|extract-all|enrich|enrich-all|validate|validate-all)
  -a, --associations   Association codes (repeatable: -a PMA -a NEMA)
  --enrichment    Enrichment type (firmographic|techstack|contacts|all)
  --validation    Validation type (dedupe|crossref|score|all)
  --dry-run       Run without saving results
  --job-id        Specific job ID
  --resume        Resume from failed job ID
  --log-level     Logging level (DEBUG|INFO|WARNING|ERROR)
  --persist-db    Also persist results to PostgreSQL (requires DATABASE_URL)
```

### 3.5 Target Associations

| Code | Name | Expected Members | Priority |
|------|------|------------------|----------|
| PMA | Precision Metalforming Association | ~900 | High |
| NEMA | National Electrical Manufacturers Association | ~300 | High |
| SOCMA | Society of Chemical Manufacturers & Affiliates | ~148 | High |
| AGMA | American Gear Manufacturers Association | ~495 | High |
| AIA | Aerospace Industries Association | ~268 | Medium |
| NTMA | National Tooling & Machining Association | ~1,400 | Medium |
| PMPA | Precision Machined Products Association | ~350 | Medium |
| FIA | Forging Industry Association | ~150 | Medium |
| NADCA | North American Die Casting Association | ~300 | Medium |
| AFS | American Foundry Society | ~500 | Medium |

---

## 4. Monitoring & Observability

### 4.1 Log Files

Logs are written in structured JSON format to `logs/`:

```bash
# Tail live logs
tail -f logs/*.log

# Search for errors
grep '"level":"ERROR"' logs/*.log | python -m json.tool

# Find specific job
grep '"job_id":"<id>"' logs/*.log
```

### 4.2 Health Check

The INIT phase writes a health summary to `data/.state/<job-id>/health_check.json`:

```json
{
  "timestamp": "2026-02-08T12:00:00+00:00",
  "job_id": "abc-123",
  "associations": ["PMA", "NEMA"],
  "api_keys": {
    "CLEARBIT_API_KEY": true,
    "APOLLO_API_KEY": true,
    "BUILTWITH_API_KEY": false
  },
  "disk_free_gb": 42.5,
  "mode": "full",
  "dry_run": false
}
```

### 4.3 Pipeline State Inspection

```bash
# View current state of a running/failed job
python -c "
import json
with open('data/.state/<job-id>/state.json') as f:
    state = json.load(f)
print(f'Phase: {state[\"current_phase\"]}')
print(f'Companies: {len(state.get(\"companies\", []))}')
print(f'Errors: {len(state.get(\"errors\", []))}')
print(f'Queue: {len(state.get(\"url_queue\", []))}')
"
```

### 4.4 Dead Letter Queue

Failed records are sent to `data/dead_letter/`. Review periodically:

```bash
# Count dead-letter items
wc -l data/dead_letter/*.jsonl

# Inspect failures
python -c "
import json
with open('data/dead_letter/failed_records.jsonl') as f:
    for line in f:
        record = json.loads(line)
        print(f'{record.get(\"error_type\")}: {record.get(\"error_message\", \"\")[:80]}')
"
```

### 4.5 Source Monitoring Reports

After the MONITOR phase, reports are saved to `data/monitoring/reports/`:

```bash
# View latest monitoring report
cat data/monitoring/reports/*.json | python -m json.tool
```

---

## 5. Troubleshooting

### 5.1 Common Failure Scenarios

#### Pipeline Fails at GATEKEEPER Phase
**Symptom:** Job fails with "access denied" or "robots.txt disallows" errors.
**Cause:** Target association website blocks automated access.
**Resolution:**
1. Check `data/.state/<job-id>/state.json` for the specific URL that failed.
2. Manually inspect the site's `robots.txt` (e.g., `curl https://pma.org/robots.txt`).
3. If the site legitimately blocks crawling, exclude it from `config/associations.yaml`.
4. If the user-agent is blocked, update `config/agents.yaml` → `access_gatekeeper.user_agent`.

#### Pipeline Fails at EXTRACTION Phase with High Error Rate
**Symptom:** `error_rate_exceeded` or `Too many extraction errors` in logs.
**Cause:** Website structure changed (DOM drift) or rate limiting is too aggressive.
**Resolution:**
1. Check the error rate threshold: `config/agents.yaml` → `orchestrator.max_extraction_errors` (default: 0.1 = 10%).
2. Inspect specific errors in the log: `grep "extraction.*error" logs/*.log`.
3. If DOM changed, update extraction selectors in the relevant agent config.
4. If rate-limited, reduce `concurrent_requests` in `config/agents.yaml` → `extraction.html_parser`.
5. Resume the job: `python -m agents.orchestrator --mode full --resume <job-id>`.

#### 429 Rate Limited Responses
**Symptom:** Logs show `429` status codes and exponential backoff messages.
**Cause:** Exceeding rate limits on target sites or APIs.
**Resolution:**
1. Check current limits in `config/agents.yaml` → `rate_limits` section.
2. Reduce the rate for the offending domain.
3. Wait before retrying — the pipeline implements automatic exponential backoff.
4. For API providers, check your quota dashboard (Clearbit, Apollo, etc.).

#### Enrichment Phase Produces Empty Results
**Symptom:** Companies have no firmographic data, tech stack, or contacts after enrichment.
**Cause:** Missing or expired API keys.
**Resolution:**
1. Check `data/.state/<job-id>/health_check.json` for API key status.
2. Verify keys in `.env` are current and have remaining quota.
3. Re-run enrichment only: `python -m agents.orchestrator --mode enrich-all`.

#### Out of Disk Space
**Symptom:** Pipeline fails at INIT with "Insufficient disk space" or OSError during writes.
**Cause:** `data/` directory has grown too large from accumulated runs.
**Resolution:**
1. The INIT health check requires ≥1 GB free disk space.
2. Archive old runs: `mv data/validated/2025-* /archive/`.
3. Clean dead letters: `rm data/dead_letter/*.jsonl`.
4. Clean old state: `rm -rf data/.state/<old-job-ids>`.

#### Database Connection Failures
**Symptom:** `--persist-db` fails with connection errors.
**Cause:** PostgreSQL is not running or `DATABASE_URL` is incorrect.
**Resolution:**
1. Verify PostgreSQL is running: `docker compose ps postgres` or `pg_isready`.
2. Check `DATABASE_URL` in `.env`.
3. Re-initialize if needed: `python scripts/init_db.py`.
4. For Docker: `docker compose restart postgres`.

### 5.2 Diagnostic Commands

```bash
# Check if all core modules import successfully
python scripts/healthcheck.py

# Verify database connectivity (requires DATABASE_URL)
python -c "
from db.connection import DatabasePool
import asyncio, os
pool = DatabasePool(os.getenv('DATABASE_URL'))
asyncio.run(pool.init())
print('Database connection OK')
asyncio.run(pool.close())
"

# Count records in data directory
find data/raw -name "*.jsonl" -exec wc -l {} +
find data/validated -name "*.jsonl" -exec wc -l {} +

# Check for stuck jobs (state files with non-DONE phase)
for f in data/.state/*/state.json; do
  phase=$(python -c "import json; print(json.load(open('$f'))['current_phase'])")
  echo "$f: $phase"
done
```

---

## 6. Rate Limit Management

### 6.1 Configured Rate Limits

Rate limits are defined in `config/agents.yaml` → `rate_limits`:

| Domain | Rate (req/sec) | Daily Cap |
|--------|---------------|-----------|
| Association sites (pma.org, etc.) | 0.5 | 1,000/day per domain |
| linkedin.com | 0.2 | 200/day |
| indeed.com | 0.3 | — |
| clearbit.com | 10.0 | 10,000/day |
| builtwith.com | 5.0 | 5,000/day |
| apollo.io | 0.8 | 5,000/day |
| zoominfo.com | 1.5 | 5,000/day |
| Default (unknown domains) | 1.0 | — |

### 6.2 Adjusting Rate Limits

Edit `config/agents.yaml`:

```yaml
rate_limits:
  pma.org: 0.3    # Reduce if getting 429s
  clearbit.com: 5  # Reduce if approaching daily quota
```

**Important:** Never exceed 1 req/sec for association websites. These are small organizations and aggressive crawling could get the pipeline permanently blocked.

### 6.3 Monitoring API Quotas

Check provider dashboards regularly:
- **Clearbit:** [dashboard.clearbit.com](https://dashboard.clearbit.com)
- **Apollo:** [app.apollo.io/settings/integrations](https://app.apollo.io/settings/integrations)
- **BuiltWith:** [builtwith.com/account](https://builtwith.com/account)
- **ZoomInfo:** Contact your account representative

---

## 7. API Key Rotation

### 7.1 Using Environment Variables (Default)

1. Generate new key from the provider dashboard.
2. Update `.env` with the new key value.
3. Restart the pipeline (no code changes needed).
4. Verify via health check: look for `"api_keys"` in `health_check.json`.

### 7.2 Using HashiCorp Vault (Production)

The pipeline supports Vault KV v2 via `middleware/secrets.py`:

```bash
# 1. Configure Vault environment
export VAULT_ADDR=https://vault.mycompany.com:8200
export VAULT_TOKEN=<token>
export VAULT_MOUNT=secret          # default
export VAULT_PATH=nam-pipeline     # default

# 2. Store secrets in Vault
vault kv put secret/nam-pipeline \
  CLEARBIT_API_KEY=<new-key> \
  APOLLO_API_KEY=<new-key> \
  BUILTWITH_API_KEY=<new-key>

# 3. Restart pipeline — it auto-detects Vault when VAULT_ADDR + VAULT_TOKEN are set
```

**Provider chain:** Vault (if configured) → Environment variables (always). First non-None result wins.

**Cache TTL:** Secrets are cached for 300 seconds (5 minutes) by default. To force a refresh, the pipeline must be restarted.

### 7.3 Key Rotation Checklist

- [ ] Generate new key in provider dashboard
- [ ] Update `.env` or Vault with new key
- [ ] Revoke old key in provider dashboard (after confirming new key works)
- [ ] Run a dry-run to verify: `python -m agents.orchestrator --mode full --dry-run -a PMA`
- [ ] Check `health_check.json` confirms key is detected

---

## 8. Database Operations

### 8.1 Schema

The database contains 10 tables + 4 views. See `scripts/init_db.py` for full DDL.

**Core Tables:**

| Table | Purpose |
|-------|---------|
| `companies` | Canonical company records with firmographics |
| `association_memberships` | Company ↔ association links |
| `contacts` | Decision-maker contacts per company |
| `extraction_jobs` | Job tracking and progress |
| `events` | Trade shows, conferences, webinars |
| `event_participants` | Sponsors, exhibitors, speakers per event |
| `competitor_signals` | ERP competitor mentions and context |
| `entity_relationships` | Graph edges between entities |
| `source_baselines` | DOM snapshots for drift detection |
| `quality_audit_log` | Change history for quality fields |

**Views:** `company_summary`, `job_summary`, `event_summary`, `competitor_report`

### 8.2 Common Queries

```sql
-- Count companies by quality grade
SELECT quality_grade, COUNT(*) FROM companies GROUP BY quality_grade ORDER BY quality_grade;

-- Find companies with known ERP systems
SELECT canonical_name, erp_system, quality_score
FROM companies WHERE erp_system IS NOT NULL ORDER BY quality_score DESC;

-- Top associations by member count
SELECT association_code, COUNT(*) as members
FROM association_memberships WHERE company_id IS NOT NULL
GROUP BY association_code ORDER BY members DESC;

-- Recent extraction jobs
SELECT * FROM job_summary LIMIT 10;

-- Competitor signal summary
SELECT * FROM competitor_report;
```

### 8.3 Database Maintenance

```bash
# Initialize fresh database
python scripts/init_db.py

# Reset (WARNING: deletes all data)
python scripts/init_db.py --drop --force

# Run Alembic migrations (if available)
alembic upgrade head
```

---

## 9. Backup & Recovery

### 9.1 Data Backup

```bash
# Back up PostgreSQL
pg_dump -U nam_user nam_intel > backup_$(date +%Y%m%d).sql

# Back up file-based state
tar czf nam_data_backup_$(date +%Y%m%d).tar.gz data/

# Back up configuration
tar czf nam_config_backup.tar.gz config/ .env
```

### 9.2 Recovery Procedures

```bash
# Restore PostgreSQL
psql -U nam_user nam_intel < backup_20260208.sql

# Restore file state
tar xzf nam_data_backup_20260208.tar.gz

# Resume interrupted pipeline
python -m agents.orchestrator --mode full --resume <job-id>
```

---

## 10. Maintenance Procedures

### 10.1 Routine Maintenance (Weekly)

- [ ] Review `data/dead_letter/` for recurring failures
- [ ] Check API quota usage on provider dashboards
- [ ] Review monitoring reports in `data/monitoring/reports/`
- [ ] Archive old validated data: `mv data/validated/<old-timestamps> /archive/`
- [ ] Clean old state files: `rm -rf data/.state/<completed-job-ids>`
- [ ] Run test suite: `pytest tests/ -v` (should pass 1,532+ tests)

### 10.2 Monthly Maintenance

- [ ] Review and rotate API keys if approaching expiry
- [ ] Check for DOM drift on high-value association sites
- [ ] Update `config/associations.yaml` if association URLs changed
- [ ] Review quality score distribution in database
- [ ] Run full pipeline in dry-run mode to detect structural changes
- [ ] Update dependencies: `pip install --upgrade -r requirements.txt`

### 10.3 Incident Response

**Severity Levels:**

| Level | Criteria | Response Time | Example |
|-------|----------|---------------|---------|
| P0 | Pipeline completely broken | Immediate | Database down, all API keys expired |
| P1 | Major data quality issue | < 4 hours | Extraction producing 0 records from a major association |
| P2 | Partial degradation | < 24 hours | One enrichment API returning errors |
| P3 | Minor issue | Next maintenance window | Slight quality score drop |

**Incident Steps:**
1. Check health: `python scripts/healthcheck.py`
2. Review latest logs: `tail -100 logs/*.log | grep ERROR`
3. Check pipeline state: inspect `data/.state/<job-id>/state.json`
4. Identify root cause using Section 5 (Troubleshooting)
5. Apply fix and resume: `--resume <job-id>`
6. Verify fix with dry-run on affected association

