# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Multi-agent data pipeline for extracting, enriching, and validating manufacturing company data from NAM-affiliated associations. Goal: Build database of 10,000+ manufacturing companies with firmographic data, tech stack detection, and decision-maker contacts for ERP sales targeting.

## Development Commands

```bash
# Setup
python -m venv venv
venv\Scripts\activate           # Windows
pip install -r requirements.txt
npm install
playwright install chromium
cp .env.example .env            # Then configure API keys
python scripts/init_db.py       # Initialize PostgreSQL

# Run Pipeline
python -m agents.orchestrator --mode extract -a PMA                    # Single association
python -m agents.orchestrator --mode extract -a PMA -a NEMA -a AGMA   # Multiple
python -m agents.orchestrator --mode extract-all                       # All associations
python -m agents.orchestrator --mode full -a PMA                       # Full pipeline
python -m agents.orchestrator --mode full --dry-run                    # Test without saving

# Enrichment & Validation
python -m agents.orchestrator --mode enrich --enrichment firmographic
python -m agents.orchestrator --mode enrich --enrichment techstack
python -m agents.orchestrator --mode enrich --enrichment contacts
python -m agents.orchestrator --mode validate --validation dedupe
python -m agents.orchestrator --mode validate --validation crossref
python -m agents.orchestrator --mode validate --validation score

# Testing
pytest                          # Run all tests
pytest tests/test_extraction.py # Single test file
npm test                        # Node tests
npm run lint                    # ESLint
```

## Architecture

### Agent Hierarchy (20 Agents)
```
Orchestrator (agents/orchestrator.py)
    ├── Discovery Agents: access_gatekeeper, site_mapper, link_crawler, page_classifier
    ├── Extraction Agents: html_parser, api_client, pdf_parser, event_extractor, event_participant_extractor
    ├── Enrichment Agents: firmographic, tech_stack, contact_finder
    ├── Validation Agents: dedupe, crossref, scorer, entity_resolver
    ├── Intelligence Agents: competitor_signal_miner, relationship_graph_builder
    ├── Export Agents: export_activation
    └── Monitoring Agents: source_monitor
```

All agents inherit from `BaseAgent` (agents/base.py) which provides:
- Config loading from `config/` YAML files
- Rate-limited HTTP client via `AsyncHTTPClient`
- Structured JSON logging
- Checkpoint/resume via `checkpoint()` and `load_checkpoint()`
- JSONL file I/O via `save_records()` and `load_records()`

### Agent Spawning
The `AgentSpawner` class in base.py handles dynamic agent loading:
- `spawn(agent_type, task)` - Run single agent
- `spawn_parallel(agent_type, tasks, max_concurrent)` - Parallel execution with semaphore

### Pipeline Flow
1. **Init**: Load config, run health check (`_build_health_summary()`)
2. **Gatekeeper**: Check robots.txt, ToS, auth requirements
3. **Discovery**: Association URL → Site Mapper → Link Crawler → `data/raw/{assoc}/urls.jsonl`
4. **Classification**: Page Classifier → route pages by type
5. **Extraction**: URLs → HTML Parser/Event Extractor → `data/raw/{assoc}/records.jsonl`
6. **Enrichment**: Records → Firmographic/TechStack/Contacts → `data/processed/enriched.jsonl`
7. **Validation**: Enriched → Dedupe/CrossRef/Scorer → `data/validated/{timestamp}/companies.jsonl`
8. **Resolution**: Entity Resolver → canonical entities
9. **Graph**: Competitor Signal Miner + Relationship Graph Builder
10. **Export**: Export Activation → CSV/JSON/CRM files
11. **Monitor**: Source Monitor → baseline tracking

## Before Performing Any Task

**Read the relevant skill file first:**
- `skills/discovery/SKILL.md` - Site mapping, link crawling
- `skills/extraction/SKILL.md` - HTML/API/PDF parsing
- `skills/enrichment/SKILL.md` - Firmographic, tech stack, contacts
- `skills/validation/SKILL.md` - Dedupe, cross-ref, scoring
- `skills/orchestration/SKILL.md` - Workflow coordination

## Critical Rules

### Rate Limiting (MUST FOLLOW)
```
Association websites: 0.5 req/sec (max 1,000/day per domain)
LinkedIn: 0.2 req/sec (max 200/day)
Clearbit API: 10 req/sec
BuiltWith API: 5 req/sec
```

### robots.txt
**ALWAYS** check and respect robots.txt before crawling any domain.

### Error Handling
| Error | Action |
|-------|--------|
| 429 Rate Limited | Wait, exponential backoff |
| 404 Not Found | Skip URL, log warning |
| 403 Forbidden | Try different headers, then skip |
| 5xx Server Error | Retry 3x with backoff |
| Parse Error | Log HTML snippet, skip |

## Data Schema

```json
{
  "company_name": "string (required)",
  "website": "string",
  "domain": "string",
  "city": "string",
  "state": "string (2-letter)",
  "country": "string (default: United States)",
  "employee_count_min": "integer",
  "employee_count_max": "integer",
  "revenue_min_usd": "integer",
  "revenue_max_usd": "integer",
  "naics_code": "string",
  "erp_system": "string",
  "crm_system": "string",
  "tech_stack": ["strings"],
  "contacts": [{"name", "title", "email", "phone"}],
  "associations": ["association codes"],
  "quality_score": "integer (0-100)"
}
```

## Database

PostgreSQL with tables: `companies`, `association_memberships`, `contacts`, `extraction_jobs`, `quality_audit_log`, `url_queue`, `duplicate_groups`. See `scripts/init_db.py` for schema.

Reset database: `python scripts/init_db.py --drop --force`

## Environment Variables

Required in `.env`:
```
DATABASE_URL=postgresql://user:pass@localhost:5432/nam_intel
CLEARBIT_API_KEY=     # Company enrichment
APOLLO_API_KEY=       # Sales intelligence
BUILTWITH_API_KEY=    # Technology detection
```

Optional: `ZOOMINFO_API_KEY`, `HUNTER_API_KEY`, `GOOGLE_PLACES_API_KEY`

## Target Associations

High priority: PMA, NEMA, SOCMA, AGMA, AIA
Medium priority: NTMA, PMPA, FIA, NADCA, AFS

Configure in `config/associations.yaml`.

## Session Handoff

After completing implementation work, update `docs/HANDOFF.md`:
- Add a new session entry at the top (after the `---` following the header)
- Include: Session Summary, Completed This Session, Files Modified, Key Decisions
- Follow the template at the bottom of HANDOFF.md
