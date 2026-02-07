# Extraction Skill

## Overview

Extraction agents parse data from various sources (HTML pages, APIs, PDFs) and output structured company records.

---

## Agents

1. **HTML Parser**: Extracts data from web pages using selectors
2. **API Client**: Fetches data from REST/GraphQL APIs
3. **PDF Parser**: Extracts data from PDF directories

---

## HTML Parser Agent

### Purpose
Extract structured company data from HTML member pages using CSS/XPath selectors.

### Input
```json
{
  "url": "https://pma.org/member/12345",
  "schema": "pma"
}
```

### Output
```json
{
  "company_name": "Acme Manufacturing Inc.",
  "website": "https://acme-mfg.com",
  "city": "Chicago",
  "state": "IL",
  "membership_tier": "Gold",
  "member_since": 2015,
  "source_url": "https://pma.org/member/12345",
  "extracted_at": "2026-01-29T12:00:00Z"
}
```

### Extraction Schema

```yaml
# config/schemas.yaml
default:
  company_name:
    selectors:
      - "h1.company-name"
      - "h2.member-title"
      - ".profile-header h1"
      - "h1"
    required: true
    
  website:
    selectors:
      - "a[rel='external']"
      - ".company-website a"
      - "a.website-link"
    extract: "href"
    
  city:
    selectors:
      - ".city"
      - "span[itemprop='addressLocality']"
    parser: "title_case"
    
  state:
    selectors:
      - ".state"
      - "span[itemprop='addressRegion']"
    parser: "state_code"
    
  membership_tier:
    selectors:
      - ".membership-level"
      - ".tier-badge"
    enum: ["Platinum", "Gold", "Silver", "Bronze", "New"]
    
  member_since:
    selectors:
      - ".member-since"
    parser: "year"

pma:
  extends: default
  company_name:
    selectors:
      - "td.company-name"
      - "a.member-link"
  membership_tier:
    mapping:
      "P": "Platinum"
      "G": "Gold"
      "S": "Silver"
```

### Process

```python
async def extract_from_url(url: str, schema: str) -> dict:
    # 1. Load schema
    schema_config = load_schema(schema)
    
    # 2. Fetch page with rate limiting
    await rate_limiter.acquire(urlparse(url).netloc)
    response = await fetch(url)
    
    if response.status_code != 200:
        raise ExtractionError(url, f"HTTP {response.status_code}")
    
    soup = BeautifulSoup(response.text, 'lxml')
    
    # 3. Extract each field
    record = {
        "source_url": url,
        "extracted_at": datetime.utcnow().isoformat()
    }
    
    for field_name, field_config in schema_config.items():
        value = extract_field(soup, field_config)
        
        # Apply parser if specified
        if field_config.get("parser"):
            value = apply_parser(value, field_config["parser"])
        
        # Apply mapping if specified
        if field_config.get("mapping") and value:
            value = field_config["mapping"].get(value, value)
        
        # Validate required fields
        if field_config.get("required") and not value:
            log.warning(f"Required field {field_name} empty for {url}")
        
        record[field_name] = value
    
    return record

def extract_field(soup: BeautifulSoup, config: dict) -> str:
    """Try each selector until one matches."""
    for selector in config.get("selectors", []):
        try:
            if selector.startswith("//"):
                # XPath
                from lxml import etree
                tree = etree.HTML(str(soup))
                elements = tree.xpath(selector)
                if elements:
                    return get_text(elements[0])
            else:
                # CSS selector
                element = soup.select_one(selector)
                if element:
                    if config.get("extract") == "href":
                        return element.get("href")
                    return get_text(element)
        except:
            continue
    return None

def get_text(element) -> str:
    """Extract clean text from element."""
    if hasattr(element, 'get_text'):
        text = element.get_text(strip=True)
    else:
        text = str(element)
    return ' '.join(text.split())  # Normalize whitespace
```

### Parsers

```python
PARSERS = {
    "title_case": lambda s: s.strip().title() if s else None,
    
    "state_code": lambda s: STATE_CODES.get(s.strip().lower(), s[:2].upper()) if s else None,
    
    "year": lambda s: int(re.search(r'(19|20)\d{2}', s).group()) if s and re.search(r'(19|20)\d{2}', s) else None,
    
    "phone": lambda s: re.sub(r'[^\d+]', '', s) if s else None,
    
    "email": lambda s: s.strip().lower() if s and '@' in s else None,
    
    "url": lambda s: s if s and s.startswith('http') else f"https://{s}" if s else None,
}

STATE_CODES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", # ... etc
}
```

### Batch Processing

```python
async def extract_batch(urls: list[str], schema: str) -> list[dict]:
    """Process multiple URLs with parallelism and error handling."""
    results = []
    errors = []
    
    for i, url in enumerate(urls):
        try:
            record = await extract_from_url(url, schema)
            results.append(record)
            
            if (i + 1) % 100 == 0:
                log.info(f"Extracted {i + 1}/{len(urls)} records")
                
        except Exception as e:
            errors.append({"url": url, "error": str(e)})
            log.warning(f"Failed to extract {url}: {e}")
    
    if errors:
        log.warning(f"Failed to extract {len(errors)} URLs")
        save_jsonl(f"data/raw/{schema}/errors.jsonl", errors)
    
    return results
```

---

## API Client Agent

### Purpose
Fetch data from REST APIs (enrichment services, association APIs).

### Input
```json
{
  "endpoint": "https://company.clearbit.com/v2/companies/find",
  "method": "GET",
  "params": {"domain": "acme-mfg.com"},
  "auth": {"type": "bearer", "key_env": "CLEARBIT_API_KEY"}
}
```

### Output
```json
{
  "employee_count": 350,
  "revenue_estimate": 75000000,
  "industry": "Manufacturing",
  "naics_code": "332710"
}
```

### API Implementations

```python
async def fetch_clearbit(domain: str) -> dict:
    """Fetch company data from Clearbit."""
    api_key = os.getenv("CLEARBIT_API_KEY")
    if not api_key:
        raise ConfigError("CLEARBIT_API_KEY not set")
    
    response = await http.get(
        "https://company.clearbit.com/v2/companies/find",
        params={"domain": domain},
        headers={"Authorization": f"Bearer {api_key}"}
    )
    
    if response.status_code == 200:
        data = response.json()
        return {
            "employee_count_min": data.get("metrics", {}).get("employees"),
            "employee_count_max": data.get("metrics", {}).get("employees"),
            "revenue_estimate": parse_revenue(data.get("metrics", {}).get("estimatedAnnualRevenue")),
            "year_founded": data.get("foundedYear"),
            "naics_code": data.get("category", {}).get("naicsCode"),
            "industry": data.get("category", {}).get("industry"),
            "linkedin_url": f"https://linkedin.com/company/{data.get('linkedin', {}).get('handle')}" if data.get("linkedin") else None,
            "source": "clearbit"
        }
    elif response.status_code == 404:
        return None  # Not found
    elif response.status_code == 429:
        raise RateLimitError("clearbit")
    else:
        raise APIError("clearbit", response.status_code)

async def fetch_builtwith(domain: str) -> dict:
    """Fetch technology stack from BuiltWith."""
    api_key = os.getenv("BUILTWITH_API_KEY")
    
    response = await http.get(
        "https://api.builtwith.com/v21/api.json",
        params={"KEY": api_key, "LOOKUP": domain}
    )
    
    if response.status_code == 200:
        data = response.json()
        technologies = []
        erp_system = None
        crm_system = None
        
        for result in data.get("Results", []):
            for path in result.get("Result", {}).get("Paths", []):
                for tech in path.get("Technologies", []):
                    name = tech.get("Name")
                    categories = tech.get("Categories", [])
                    technologies.append(name)
                    
                    if any("ERP" in c for c in categories):
                        erp_system = name
                    if any("CRM" in c for c in categories):
                        crm_system = name
        
        return {
            "tech_stack": technologies,
            "erp_system": erp_system,
            "crm_system": crm_system,
            "source": "builtwith"
        }
    return None
```

---

## PDF Parser Agent

### Purpose
Extract member data from PDF directories and annual reports.

### Input
```json
{
  "pdf_url": "https://association.org/member-directory-2026.pdf"
}
```

### Output
```json
{
  "records": [
    {"company_name": "Acme Inc.", "city": "Chicago", "state": "IL"}
  ],
  "pages_processed": 45
}
```

### Process

```python
import pdfplumber

async def extract_from_pdf(pdf_source: str) -> list[dict]:
    # Download if URL
    if pdf_source.startswith('http'):
        response = await http.get(pdf_source)
        pdf_bytes = response.content
    else:
        pdf_bytes = Path(pdf_source).read_bytes()
    
    records = []
    
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            # Try table extraction first
            tables = page.extract_tables()
            
            if tables:
                for table in tables:
                    records.extend(parse_table(table))
            else:
                # Fall back to text extraction
                text = page.extract_text()
                if text:
                    records.extend(parse_text(text))
    
    return records

def parse_table(table: list[list]) -> list[dict]:
    """Parse table rows into records."""
    if not table or len(table) < 2:
        return []
    
    # First row = headers
    headers = [normalize_header(h) for h in table[0]]
    
    records = []
    for row in table[1:]:
        if len(row) != len(headers):
            continue
        
        record = {}
        for header, value in zip(headers, row):
            if header and value:
                record[header] = value.strip()
        
        if record.get('company_name'):
            records.append(record)
    
    return records
```

---

## Output Format

Save to: `data/raw/{association}/records.jsonl`

Each line:
```json
{"company_name": "Acme Inc.", "website": "https://acme.com", "city": "Chicago", "state": "IL", "source_url": "https://...", "extracted_at": "2026-01-29T12:00:00Z"}
```

---

## Error Handling

| Error | Action |
|-------|--------|
| 404 Not Found | Skip, log warning |
| 403 Forbidden | Try different headers, then skip |
| Timeout | Retry 3x with backoff |
| Parse Error | Log HTML snippet, skip |
| Empty Required Field | Log warning, include partial record |

---

## Best Practices

1. **Use multiple selectors** - Sites change, have fallbacks
2. **Validate extracted data** - Check required fields
3. **Hash raw HTML** - Enable change detection later
4. **Log extensively** - Include URL and matched selector
5. **Batch wisely** - 100-500 URLs per batch
6. **Save raw data** - Keep for debugging and re-extraction
