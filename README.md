# NAM Competitive Intelligence Pipeline

> Multi-agent data pipeline for manufacturing company intelligence extraction, enrichment, and validation.

## 🎯 Overview

This pipeline automates the collection and enrichment of manufacturing company data from National Association of Manufacturers (NAM) affiliated associations. It's designed for sales and marketing teams targeting the manufacturing ERP market.

### What It Does

1. **Extracts** company data from 10+ association member directories
2. **Enriches** records with firmographic data, technology stack detection, and decision-maker contacts
3. **Validates** data quality through deduplication, cross-referencing, and scoring

### Key Metrics

| Metric | Target |
|--------|--------|
| Total Companies | 10,000+ |
| Associations Covered | 20+ |
| Records with Website | 95%+ |
| ERP Detection Rate | 30%+ |
| Contact Coverage | 50%+ |
| Data Quality Score | >75 avg |

---

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- Node.js 18+
- PostgreSQL 14+
- Redis 6+

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/your-org/nam-intelligence-pipeline.git
cd nam-intelligence-pipeline

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# 3. Install Python dependencies
pip install -r requirements.txt

# 4. Install Node dependencies
npm install

# 5. Install Playwright browsers (for JS-heavy sites)
playwright install chromium

# 6. Copy environment file and configure
cp .env.example .env
# Edit .env with your API keys and database credentials

# 7. Initialize database
python scripts/init_db.py

# 8. Run a test extraction
python -m agents.orchestrator --mode extract --associations PMA --dry-run
```

### First Run

```bash
# Extract data from high-priority associations
python -m agents.orchestrator --mode extract --associations PMA,NEMA,AGMA

# Run full pipeline (extract + enrich + validate)
python -m agents.orchestrator --mode full --associations PMA
```

---

## 📁 Project Structure

```
nam-intelligence-pipeline/
├── CLAUDE.md                 # Claude Code project instructions
├── README.md                 # This file
├── requirements.txt          # Python dependencies
├── package.json              # Node dependencies
├── .env.example              # Environment template
│
├── agents/                   # Agent implementations (20 agents)
│   ├── __init__.py
│   ├── base.py              # Base agent class + AgentSpawner
│   ├── orchestrator.py      # Main orchestrator (state machine)
│   ├── discovery/           # Site mapper, link crawler, page classifier, access gatekeeper
│   ├── extraction/          # HTML parser, API client, PDF parser, event extractor, event participant extractor
│   ├── enrichment/          # Firmographic, tech stack, contact finder
│   ├── validation/          # Dedupe, cross-ref, scorer, entity resolver
│   ├── intelligence/        # Competitor signal miner, relationship graph builder
│   ├── export/              # Export activation
│   └── monitoring/          # Source monitor
│
├── middleware/               # Cross-cutting concerns
│   ├── policy.py            # Crawl policy enforcement
│   └── secrets.py           # Secrets manager (Vault + env fallback)
│
├── contracts/                # Data contracts
│   ├── validator.py         # JSON Schema validation
│   └── schemas/             # 48 JSON schema files
│
├── models/                   # Data models
│   └── ontology.py          # Pydantic models (Company, Contact, etc.)
│
├── state/                    # Pipeline state management
│   └── machine.py           # State machine, PipelineState, StateManager
│
├── db/                       # Database layer
│   ├── models.py            # SQLAlchemy models
│   └── repository.py        # Data access layer
│
├── skills/                   # Agent skill documentation
│   ├── orchestrator/SKILL.md
│   ├── discovery/SKILL.md
│   ├── extraction/SKILL.md
│   ├── enrichment/SKILL.md
│   ├── validation/SKILL.md
│   └── common/SKILL.py      # Shared utilities (AsyncHTTPClient, RateLimiter, etc.)
│
├── config/                   # Configuration files
│   ├── associations.yaml    # Association definitions
│   └── agents.yaml          # Agent settings
│
├── scripts/                  # Utility scripts
│   └── init_db.py           # Database initialization
│
├── tests/                    # Test suite (1,520 tests)
│   ├── conftest.py          # Shared fixtures
│   └── test_*.py            # 34 test files
│
├── data/                     # Data storage
│   ├── raw/                 # Raw extracted data
│   ├── processed/           # Enriched data
│   └── validated/           # Final validated data
│
└── logs/                     # Execution logs
```

---

## 🤖 Agent Architecture (20 Agents)

### Coordination Layer

| Agent | Purpose |
|-------|---------|
| **Orchestrator** | Central coordinator, state machine, workflow management |

### Discovery Layer

| Agent | Purpose |
|-------|---------|
| **Access Gatekeeper** | Check robots.txt, ToS, auth requirements before crawling |
| **Site Mapper** | Analyze websites, find member directories and event pages |
| **Link Crawler** | Follow pagination, collect URLs, handle JavaScript rendering |
| **Page Classifier** | Classify page types to route to correct extraction agent |

### Extraction Layer

| Agent | Purpose |
|-------|---------|
| **HTML Parser** | Extract structured company data from web pages |
| **API Client** | Call external enrichment APIs |
| **PDF Parser** | Extract from PDF directories and documents |
| **Event Extractor** | Extract event data (conferences, trade shows, webinars) |
| **Event Participant Extractor** | Extract sponsors, exhibitors, attendees from event pages |

### Enrichment Layer

| Agent | Purpose |
|-------|---------|
| **Firmographic** | Add company size, revenue, industry, NAICS codes |
| **Tech Stack** | Detect ERP, CRM, MES software via BuiltWith/job postings |
| **Contact Finder** | Find decision-makers via Apollo/ZoomInfo |

### Validation Layer

| Agent | Purpose |
|-------|---------|
| **Dedupe** | Merge duplicate records using edit distance + fuzzy matching |
| **Cross-Reference** | Validate against DNS, Google Places, LinkedIn (Proxycurl) |
| **Quality Scorer** | Assign quality scores (0-100) with letter grades (A-F) |
| **Entity Resolver** | Consolidate records into canonical entities with confidence scores |

### Intelligence Layer

| Agent | Purpose |
|-------|---------|
| **Competitor Signal Miner** | Detect competitor ERP/software mentions in association content |
| **Relationship Graph Builder** | Map company-association-event relationships |

### Export Layer

| Agent | Purpose |
|-------|---------|
| **Export Activation** | Generate CSV/JSON/CRM exports with provenance tracking |

### Monitoring Layer

| Agent | Purpose |
|-------|---------|
| **Source Monitor** | Detect DOM drift, track source health, alert on changes |

---

## ⚙️ Configuration

### Environment Variables

Key variables in `.env`:

```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/nam_intel

# Enrichment APIs
CLEARBIT_API_KEY=your_key      # Company enrichment
ZOOMINFO_API_KEY=your_key      # B2B intelligence
APOLLO_API_KEY=your_key        # Sales intelligence
BUILTWITH_API_KEY=your_key     # Technology detection
HUNTER_API_KEY=your_key        # Email verification
GOOGLE_PLACES_API_KEY=your_key # Address validation
```

### Association Configuration

Edit `config/associations.yaml` to add/modify associations:

```yaml
associations:
  PMA:
    name: "Precision Metalforming Association"
    url: "https://pma.org"
    directory_url: "https://pma.org/directory/results.asp?n=2000"
    expected_members: 1134
    priority: "high"
```

---

## 🔧 CLI Commands

### Extraction

```bash
# Extract single association
python -m agents.orchestrator --mode extract -a PMA

# Extract multiple associations
python -m agents.orchestrator --mode extract -a PMA -a NEMA -a AGMA

# Extract all configured associations
python -m agents.orchestrator --mode extract-all
```

### Enrichment

```bash
# Full enrichment (firmographic + tech + contacts)
python -m agents.orchestrator --mode enrich-all

# Specific enrichment type
python -m agents.orchestrator --mode enrich --enrichment firmographic
python -m agents.orchestrator --mode enrich --enrichment techstack
python -m agents.orchestrator --mode enrich --enrichment contacts
```

### Validation

```bash
# Full validation (dedupe + crossref + score)
python -m agents.orchestrator --mode validate-all

# Specific validation
python -m agents.orchestrator --mode validate --validation dedupe
python -m agents.orchestrator --mode validate --validation crossref
python -m agents.orchestrator --mode validate --validation score
```

### Full Pipeline

```bash
# Run complete pipeline
python -m agents.orchestrator --mode full -a PMA -a NEMA

# Dry run (no data saved)
python -m agents.orchestrator --mode full --dry-run
```

---

## 📊 Output Format

### Final Records

Located in `data/validated/{timestamp}/companies.jsonl`:

```json
{
  "company_name": "Acme Manufacturing Inc.",
  "website": "https://acme-mfg.com",
  "domain": "acme-mfg.com",
  "city": "Chicago",
  "state": "IL",
  "employee_count_min": 201,
  "employee_count_max": 500,
  "revenue_min_usd": 50000000,
  "naics_code": "332710",
  "industry": "Machine Shops",
  "erp_system": "Epicor",
  "crm_system": "Salesforce",
  "contacts": [
    {
      "name": "John Smith",
      "title": "VP of IT",
      "email": "jsmith@acme-mfg.com"
    }
  ],
  "quality_score": 82,
  "quality_grade": "B",
  "associations": ["PMA", "NTMA"]
}
```

### Summary Report

Located in `data/validated/{timestamp}/summary.json`:

```json
{
  "total_records": 4443,
  "quality_distribution": {
    "A": 892,
    "B": 1567,
    "C": 1234,
    "D": 750
  },
  "average_quality_score": 78.3,
  "field_completeness": {
    "company_name": 1.0,
    "website": 0.94,
    "erp_system": 0.31
  },
  "erp_distribution": {
    "Unknown": 3000,
    "SAP": 412,
    "Epicor": 289
  }
}
```

---

## 💰 Cost Estimation

### Monthly API Costs

| Service | Volume | Unit Cost | Monthly |
|---------|--------|-----------|---------|
| Clearbit | 5,000 | $0.05 | $250 |
| ZoomInfo | 5,000 | $0.10 | $500 |
| Apollo.io | 2,500 | $0.08 | $200 |
| BuiltWith | 5,000 | $0.02 | $100 |
| Hunter.io | 5,000 | $0.01 | $50 |
| Google Places | 10,000 | $0.003 | $30 |
| **Total** | | | **$1,130** |

---

## 🔒 Rate Limiting

**CRITICAL**: Always respect rate limits to avoid IP blocks.

| Domain | Rate Limit | Daily Max |
|--------|------------|-----------|
| Association sites | 0.5 req/sec | 1,000 |
| LinkedIn | 0.2 req/sec | 200 |
| API providers | Per plan | Per plan |

---

## 🐛 Troubleshooting

### Common Issues

**Database connection failed**
```bash
# Check PostgreSQL is running
pg_isready -h localhost -p 5432

# Verify DATABASE_URL in .env
```

**API rate limited**
```bash
# Check logs for rate limit errors
grep "rate limit" logs/*.jsonl

# Reduce concurrent agents in config/agents.yaml
```

**Extraction returns empty**
```bash
# Check if site structure changed
# Review extraction schema in config/schemas/
# Test with single URL first
```

---

## 📈 Monitoring

### Logs

- `logs/hooks_YYYYMMDD.jsonl` - Agent lifecycle events
- `logs/errors_YYYYMMDD.jsonl` - Error details
- `logs/metrics.jsonl` - Performance metrics

### Health Checks

```bash
# Check database
python -c "from scripts.init_db import get_connection; get_connection()"

# Check Redis
redis-cli ping

# Test API keys
python scripts/test_apis.py
```

---

## 🤝 Contributing

1. Read `CLAUDE.md` for project conventions
2. Read relevant `skills/*/SKILL.md` for agent guidelines
3. Test changes with `--dry-run` flag
4. Submit PR with extraction logs

---

## 📄 License

Proprietary - Internal Use Only

---

## 📞 Support

For issues:
1. Check `logs/` for error details
2. Review `skills/*/SKILL.md` for agent documentation
3. Contact: intelligence-team@yourcompany.com
