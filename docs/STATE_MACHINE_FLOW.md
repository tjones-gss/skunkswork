# State Machine Data Flow

This document describes the data flow through each phase of the NAM Intelligence Pipeline state machine. Understanding these transitions is critical for debugging, extending, and validating the pipeline.

## Overview

The pipeline processes data through 12 phases, from `INIT` to `DONE`. Each phase:
- Reads from specific state buckets
- Executes one or more agents
- Writes to specific state buckets
- Updates progress counters

```
INIT → GATEKEEPER → DISCOVERY → CLASSIFICATION → EXTRACTION → ENRICHMENT
     → VALIDATION → RESOLUTION → GRAPH → EXPORT → MONITOR → DONE
```

## State Buckets

The pipeline maintains these data buckets (defined in `state/machine.py`):

| Bucket | Type | Description |
|--------|------|-------------|
| `crawl_queue` | list[dict] | URLs pending fetch with priority/depth |
| `visited_urls` | list[str] | URLs already fetched |
| `blocked_urls` | list[str] | URLs blocked by robots.txt/auth |
| `pages` | list[dict] | Fetched page snapshots |
| `companies` | list[dict] | Extracted company records |
| `events` | list[dict] | Extracted event records |
| `participants` | list[dict] | Extracted event participants |
| `competitor_signals` | list[dict] | Detected competitor mentions |
| `canonical_entities` | list[dict] | Resolved/deduplicated entities |
| `graph_edges` | list[dict] | Relationship graph edges |
| `exports` | list[dict] | Generated export file metadata |
| `errors` | list[dict] | Error records for debugging |

---

## Phase Details

### INIT Phase

**Purpose:** Initialize pipeline state with seed URLs from configuration.

**Input:**
- `association_codes` from orchestrator task
- Seed URLs from `config/associations.yaml`

**Agent:** None (orchestrator handles directly)

**Actions:**
1. Load association configurations
2. For each association:
   - Get `seed_urls` from config
   - Add to `crawl_queue` with metadata

**Output:**
- **crawl_queue:** Populated with seed URLs
- **State updates:**
  - `total_urls_discovered` incremented
  - `current_phase` → `GATEKEEPER`

**Example crawl_queue item:**
```json
{
  "url": "https://pma.org/membership/directory",
  "priority": 10,
  "depth": 0,
  "association": "PMA",
  "page_type_hint": "MEMBER_DIRECTORY"
}
```

---

### GATEKEEPER Phase

**Purpose:** Verify crawling permission for each unique domain.

**Input:**
- `crawl_queue` URLs (unique domains extracted)

**Agent:** `AccessGatekeeperAgent`

**Actions:**
1. Extract unique domains from `crawl_queue`
2. For each domain:
   - Check `robots.txt`
   - Detect authentication requirements
   - Review ToS if configured
3. Generate `AccessVerdict` for each

**Output:**
- **blocked_urls:** URLs that failed access check
- **crawl_queue:** Filtered to only allowed URLs
- **State updates:**
  - `current_phase` → `DISCOVERY`

**Contract:** `contracts/schemas/discovery/access_gatekeeper_*.json`

---

### DISCOVERY Phase

**Purpose:** Map association websites and discover member directory pages.

**Input:**
- `crawl_queue` (allowed URLs)

**Agents:**
1. `SiteMapperAgent` - Maps site structure
2. `LinkCrawlerAgent` - Fetches pages

**Actions:**
1. SiteMapper analyzes site structure
   - Check for sitemap.xml
   - Identify navigation patterns
   - Discover member directory URLs
   - Discover event page URLs
2. LinkCrawler fetches discovered URLs
   - Store HTML to `data/raw/{assoc}/pages/`
   - Calculate content hashes
   - Extract additional links

**Output:**
- **crawl_queue:** Newly discovered URLs added
- **visited_urls:** Fetched URLs recorded
- **pages:** Page snapshots added
- **State updates:**
  - `total_urls_discovered` incremented
  - `total_pages_fetched` incremented
  - `current_phase` → `CLASSIFICATION`

**Contract:** `contracts/schemas/discovery/site_mapper_*.json`, `link_crawler_*.json`

---

### CLASSIFICATION Phase

**Purpose:** Classify fetched pages to determine extraction strategy.

**Input:**
- `pages` (fetched page snapshots)
- Pages without `page_type` assigned

**Agent:** `PageClassifierAgent`

**Actions:**
1. Load HTML content from `content_path`
2. Analyze page structure:
   - URL patterns
   - DOM structure
   - Content indicators
3. Assign `PageType` classification
4. Recommend appropriate extractor

**Output:**
- **pages:** Updated with `page_type` and `recommended_extractor`
- **crawl_queue:** Items enriched with `page_type`
- **State updates:**
  - `current_phase` → `EXTRACTION`

**Contract:** `contracts/schemas/discovery/page_classifier_*.json`

**Page Type Routing:**
| Page Type | Recommended Extractor |
|-----------|----------------------|
| MEMBER_DIRECTORY | `html_parser` |
| MEMBER_DETAIL | `html_parser` |
| EVENTS_LIST | `event_extractor` |
| EVENT_DETAIL | `event_extractor` |
| EXHIBITORS_LIST | `event_participant_extractor` |
| SPONSORS_LIST | `event_participant_extractor` |

---

### EXTRACTION Phase

**Purpose:** Extract structured data from classified pages.

**Input:**
- `pages` with `page_type` assigned
- HTML content from `content_path`

**Agents:**
- `HTMLParserAgent` - Member directories/details
- `EventExtractorAgent` - Event pages
- `EventParticipantExtractorAgent` - Sponsor/exhibitor pages
- `APIClientAgent` - Associations with APIs
- `PDFParserAgent` - PDF member lists

**Actions:**
1. Route pages to appropriate extractor by `page_type`
2. Load extraction schema from `config/schemas/{assoc}.yaml`
3. Parse HTML using CSS selectors
4. Create entity records with provenance
5. Discover links to detail pages (add to queue)

**Output:**
- **companies:** Extracted company records
- **events:** Extracted event records
- **participants:** Extracted participant records
- **crawl_queue:** Detail page URLs added
- **State updates:**
  - `total_companies_extracted` incremented
  - `total_events_extracted` incremented
  - `total_participants_extracted` incremented
  - `current_phase` → `ENRICHMENT`

**Contract:** `contracts/schemas/extraction/*_*.json`

**Company record with provenance:**
```json
{
  "id": "uuid",
  "company_name": "Acme Manufacturing",
  "website": "https://acmemfg.com",
  "city": "Cleveland",
  "state": "OH",
  "associations": ["PMA"],
  "provenance": [{
    "source_url": "https://pma.org/directory/acme",
    "extracted_at": "2024-01-15T10:30:00Z",
    "extracted_by": "extraction.html_parser",
    "association_code": "PMA",
    "page_type": "MEMBER_DETAIL",
    "confidence": 0.95
  }]
}
```

---

### ENRICHMENT Phase

**Purpose:** Enrich company records with firmographic data, tech stack, and contacts.

**Input:**
- `companies` bucket

**Agents:**
1. `FirmographicAgent` - Employee count, revenue, industry
2. `TechStackAgent` - ERP, CRM, tech detection
3. `ContactFinderAgent` - Decision-maker contacts

**Actions:**
1. FirmographicAgent:
   - Query Clearbit/Apollo by domain
   - Populate employee count, revenue, industry
2. TechStackAgent:
   - Query BuiltWith by domain
   - Detect ERP/CRM systems
   - Flag competitor technologies
3. ContactFinderAgent:
   - Query Apollo/Hunter by domain
   - Find decision-maker contacts
   - Verify email deliverability

**Output:**
- **companies:** Updated with enrichment data
- **competitor_signals:** Tech-based signals detected
- **State updates:**
  - `current_phase` → `VALIDATION`

**Contract:** `contracts/schemas/enrichment/*_*.json`

---

### VALIDATION Phase

**Purpose:** Deduplicate, cross-reference, and score company records.

**Input:**
- `companies` bucket

**Agents:**
1. `DedupeAgent` - Find and merge duplicates
2. `CrossRefAgent` - Validate data accuracy
3. `ScorerAgent` - Assign quality scores

**Actions:**
1. DedupeAgent:
   - Fuzzy match on company name + domain
   - Create duplicate groups
   - Merge records based on strategy
2. CrossRefAgent:
   - Verify domains resolve
   - Check email domain matches
   - Validate addresses
3. ScorerAgent:
   - Calculate completeness score
   - Assign quality grade (A-F)

**Output:**
- **companies:** Deduplicated, validated, scored
- **State updates:**
  - `current_phase` → `RESOLUTION`

**Contract:** `contracts/schemas/validation/*_*.json`

---

### RESOLUTION Phase

**Purpose:** Resolve companies to canonical entities and link participants.

**Input:**
- `companies` bucket
- `participants` bucket
- Existing canonical entities (if any)

**Agent:** `EntityResolverAgent`

**Actions:**
1. Create canonical entity for each unique company
2. Build alias mappings (name variants)
3. Link participants to canonical companies
4. Merge company data from multiple sources

**Output:**
- **canonical_entities:** Resolved company entities
- **participants:** Updated with `company_id` links
- **State updates:**
  - `total_entities_resolved` incremented
  - `current_phase` → `GRAPH`

**Contract:** `contracts/schemas/validation/entity_resolver_*.json`

---

### GRAPH Phase

**Purpose:** Build relationship graph connecting entities.

**Input:**
- `canonical_entities`
- `events`
- `participants`
- `competitor_signals`

**Agents:**
1. `CompetitorSignalMinerAgent` - Extract competitor mentions
2. `RelationshipGraphBuilderAgent` - Build graph

**Actions:**
1. CompetitorSignalMiner:
   - Scan pages for competitor mentions
   - Detect competitor ERP/CRM usage
   - Create signal records
2. RelationshipGraphBuilder:
   - Create nodes (companies, events, associations)
   - Create edges (memberships, participation, tech usage)

**Output:**
- **competitor_signals:** All detected signals
- **graph_edges:** Relationship edges
- **State updates:**
  - `total_signals_detected` incremented
  - `current_phase` → `EXPORT`

**Contract:** `contracts/schemas/intelligence/*_*.json`

---

### EXPORT Phase

**Purpose:** Generate marketing-ready export files.

**Input:**
- All data buckets
- Export configuration

**Agent:** `ExportActivationAgent`

**Actions:**
1. Filter companies by quality threshold
2. Generate CSV exports for marketing
3. Generate JSON exports for API
4. Include provenance summaries

**Output:**
- **exports:** Export file metadata
- Files written to `data/validated/{timestamp}/`
- **State updates:**
  - `current_phase` → `MONITOR`

**Contract:** `contracts/schemas/export/export_activation_*.json`

**Export files generated:**
- `companies_full.csv` - All companies with all fields
- `companies_marketing.csv` - Marketing-ready subset
- `contacts.csv` - Decision-maker contacts
- `events.csv` - Events and dates
- `competitor_report.csv` - Competitor intelligence

---

### MONITOR Phase

**Purpose:** Create baselines for detecting source changes.

**Input:**
- `visited_urls` (directory pages)

**Agent:** `SourceMonitorAgent`

**Actions:**
1. Create DOM structure baselines
2. Record expected item counts
3. Set up drift detection thresholds

**Output:**
- Baseline snapshots persisted
- **State updates:**
  - `current_phase` → `DONE`

**Contract:** `contracts/schemas/monitoring/source_monitor_*.json`

---

### DONE Phase

**Purpose:** Terminal state indicating successful completion.

**Actions:**
1. Build final result summary
2. Calculate pipeline statistics
3. Persist final state

**Final Result:**
```json
{
  "job_id": "uuid",
  "status": "completed",
  "associations": ["PMA", "NEMA"],
  "stats": {
    "urls_discovered": 1250,
    "pages_fetched": 890,
    "companies_extracted": 2456,
    "events_extracted": 34,
    "participants_extracted": 567,
    "signals_detected": 89,
    "entities_resolved": 2301
  },
  "exports": ["companies_full.csv", ...],
  "duration_seconds": 3600
}
```

---

### FAILED Phase

**Purpose:** Terminal state indicating pipeline failure.

**Triggers:**
- Unhandled exception in any phase
- Critical validation failure
- Manual abort

**Actions:**
1. Record error details
2. Persist partial state for recovery
3. Generate error report

---

## State Transitions

Valid transitions are defined in `state/machine.py`:

```
INIT → GATEKEEPER | FAILED
GATEKEEPER → DISCOVERY | FAILED
DISCOVERY → CLASSIFICATION | FAILED
CLASSIFICATION → EXTRACTION | FAILED
EXTRACTION → ENRICHMENT | FAILED
ENRICHMENT → VALIDATION | FAILED
VALIDATION → RESOLUTION | FAILED
RESOLUTION → GRAPH | FAILED
GRAPH → EXPORT | FAILED
EXPORT → MONITOR | DONE | FAILED
MONITOR → DONE | FAILED
DONE → (terminal)
FAILED → (terminal)
```

## Checkpoint/Resume

The pipeline supports checkpoint/resume:

1. **Automatic checkpoints:** Created at each phase transition
2. **State file:** `data/.state/{job_id}.state.json`
3. **Phase checkpoints:** `data/.state/{job_id}.{phase}.checkpoint.json`

To resume a failed pipeline:
```bash
python -m agents.orchestrator --resume {job_id}
```

## Data Flow Diagram

```
┌──────────┐
│   INIT   │
└────┬─────┘
     │ seed URLs → crawl_queue
     ▼
┌────────────┐
│ GATEKEEPER │
└────┬───────┘
     │ filter blocked → blocked_urls
     ▼
┌───────────┐
│ DISCOVERY │
└────┬──────┘
     │ fetch → pages, visited_urls
     ▼
┌────────────────┐
│ CLASSIFICATION │
└────┬───────────┘
     │ classify → pages[page_type]
     ▼
┌────────────┐
│ EXTRACTION │
└────┬───────┘
     │ parse → companies, events, participants
     ▼
┌────────────┐
│ ENRICHMENT │
└────┬───────┘
     │ enrich → companies[firmographic, tech, contacts]
     ▼
┌────────────┐
│ VALIDATION │
└────┬───────┘
     │ validate → companies[quality_score]
     ▼
┌────────────┐
│ RESOLUTION │
└────┬───────┘
     │ resolve → canonical_entities
     ▼
┌───────┐
│ GRAPH │
└───┬───┘
    │ build → competitor_signals, graph_edges
    ▼
┌────────┐
│ EXPORT │
└───┬────┘
    │ generate → exports
    ▼
┌─────────┐
│ MONITOR │
└───┬─────┘
    │ baseline → (external storage)
    ▼
┌──────┐
│ DONE │
└──────┘
```
