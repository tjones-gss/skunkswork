# Enrichment Skill

## Overview

Enrichment agents add valuable data to raw company records by querying external sources. This includes firmographic data, technology stack detection, and decision-maker identification.

---

## Agents

1. **Firmographic Agent**: Adds company size, revenue, industry classification
2. **Tech Stack Agent**: Detects ERP, CRM, MES, and other business software
3. **Contact Finder Agent**: Identifies key decision-makers

---

## Firmographic Agent

### Purpose
Enrich company records with firmographic data from third-party providers.

### Input
```json
{
  "records": [{"company_name": "Acme Inc.", "website": "https://acme.com"}],
  "providers": ["clearbit", "zoominfo", "apollo"]
}
```

### Output
```json
{
  "records": [{
    "company_name": "Acme Inc.",
    "website": "https://acme.com",
    "employee_count_min": 201,
    "employee_count_max": 500,
    "revenue_min_usd": 50000000,
    "revenue_max_usd": 100000000,
    "year_founded": 1985,
    "naics_code": "332710",
    "industry": "Machine Shops",
    "firmographic_source": "clearbit"
  }],
  "match_rate": 0.82
}
```

### Process

```python
async def enrich_firmographics(records: list[dict], providers: list[str]) -> list[dict]:
    enriched = []
    matched = 0
    
    for record in records:
        domain = extract_domain(record.get("website"))
        company_name = record.get("company_name")
        
        if not domain and not company_name:
            enriched.append(record)
            continue
        
        # Try providers in order
        firmographic_data = None
        for provider in providers:
            try:
                if provider == "clearbit" and domain:
                    firmographic_data = await fetch_clearbit(domain)
                elif provider == "zoominfo" and company_name:
                    firmographic_data = await fetch_zoominfo(company_name, domain)
                elif provider == "apollo" and domain:
                    firmographic_data = await fetch_apollo(domain)
                
                if firmographic_data:
                    matched += 1
                    break
                    
            except RateLimitError:
                log.warning(f"Rate limited by {provider}")
                await asyncio.sleep(60)
            except APIError as e:
                log.warning(f"{provider} error: {e}")
        
        # Merge data
        if firmographic_data:
            record = {**record, **firmographic_data}
        
        enriched.append(record)
    
    log.info(f"Enriched {matched}/{len(records)} records ({matched/len(records)*100:.1f}%)")
    return enriched

def extract_domain(url: str) -> str:
    """Extract clean domain from URL."""
    if not url:
        return None
    if not url.startswith('http'):
        url = f"https://{url}"
    parsed = urlparse(url)
    domain = parsed.netloc.lower()
    if domain.startswith('www.'):
        domain = domain[4:]
    return domain
```

### Provider Implementations

```python
# Clearbit
async def fetch_clearbit(domain: str) -> dict:
    response = await http.get(
        "https://company.clearbit.com/v2/companies/find",
        params={"domain": domain},
        headers={"Authorization": f"Bearer {os.getenv('CLEARBIT_API_KEY')}"}
    )
    if response.status_code == 200:
        data = response.json()
        return {
            "employee_count_min": data.get("metrics", {}).get("employees"),
            "employee_count_max": data.get("metrics", {}).get("employees"),
            "revenue_min_usd": parse_revenue(data.get("metrics", {}).get("estimatedAnnualRevenue")),
            "year_founded": data.get("foundedYear"),
            "naics_code": data.get("category", {}).get("naicsCode"),
            "industry": data.get("category", {}).get("industry"),
            "linkedin_url": data.get("linkedin", {}).get("handle"),
            "firmographic_source": "clearbit"
        }
    return None

# ZoomInfo
async def fetch_zoominfo(company_name: str, domain: str = None) -> dict:
    params = {"companyName": company_name}
    if domain:
        params["domain"] = domain
    
    response = await http.get(
        "https://api.zoominfo.com/search/company",
        params=params,
        headers={"Authorization": f"Bearer {os.getenv('ZOOMINFO_API_KEY')}"}
    )
    if response.status_code == 200:
        data = response.json().get("data", [{}])[0]
        return {
            "employee_count_min": data.get("employeeCount"),
            "employee_count_max": data.get("employeeCount"),
            "revenue_min_usd": (data.get("revenueInMillions") or 0) * 1_000_000,
            "year_founded": data.get("yearFounded"),
            "naics_code": data.get("naicsCode"),
            "firmographic_source": "zoominfo"
        }
    return None

# Apollo
async def fetch_apollo(domain: str) -> dict:
    response = await http.post(
        "https://api.apollo.io/v1/organizations/enrich",
        headers={"X-Api-Key": os.getenv("APOLLO_API_KEY")},
        json={"domain": domain}
    )
    if response.status_code == 200:
        data = response.json().get("organization", {})
        return {
            "employee_count_min": data.get("estimated_num_employees"),
            "employee_count_max": data.get("estimated_num_employees"),
            "year_founded": data.get("founded_year"),
            "industry": data.get("industry"),
            "linkedin_url": data.get("linkedin_url"),
            "firmographic_source": "apollo"
        }
    return None
```

---

## Tech Stack Agent

### Purpose
Detect ERP, CRM, MES, and other business software used by companies.

### Input
```json
{
  "records": [{"company_name": "Acme Inc.", "website": "https://acme.com"}],
  "methods": ["builtwith", "job_postings", "website_fingerprint"]
}
```

### Output
```json
{
  "records": [{
    "company_name": "Acme Inc.",
    "erp_system": "Epicor",
    "crm_system": "Salesforce",
    "tech_stack": ["WordPress", "Google Analytics", "Salesforce"],
    "tech_source": "builtwith"
  }],
  "detection_rate": 0.31
}
```

### ERP Keywords

```python
ERP_KEYWORDS = {
    "sap": ["SAP", "SAP S/4HANA", "SAP ECC", "SAP Business One", "SAP B1"],
    "oracle": ["Oracle ERP", "Oracle Cloud", "JD Edwards", "NetSuite", "Oracle NetSuite"],
    "epicor": ["Epicor", "Epicor Kinetic", "Epicor Prophet 21", "Epicor Eclipse"],
    "infor": ["Infor", "Infor CloudSuite", "Infor M3", "Infor LN", "SyteLine", "Infor SyteLine"],
    "microsoft": ["Dynamics 365", "Dynamics AX", "Dynamics NAV", "Business Central", "D365"],
    "syspro": ["SYSPRO"],
    "plex": ["Plex", "Plex Manufacturing", "Plex MES"],
    "acumatica": ["Acumatica"],
    "qad": ["QAD", "QAD Adaptive ERP"],
    "ifs": ["IFS", "IFS Applications", "IFS Cloud"],
    "global_shop": ["Global Shop Solutions", "GSS ERP"],
    "sage": ["Sage", "Sage X3", "Sage Intacct", "Sage 100"],
}
```

### Detection Methods

```python
async def detect_tech_stack(records: list[dict], methods: list[str]) -> list[dict]:
    detected = 0
    
    for record in records:
        domain = extract_domain(record.get("website"))
        if not domain:
            continue
        
        tech_data = None
        
        for method in methods:
            if method == "builtwith":
                tech_data = await detect_via_builtwith(domain)
            elif method == "job_postings":
                tech_data = await detect_via_job_postings(record.get("company_name"))
            elif method == "website_fingerprint":
                tech_data = await detect_via_fingerprint(domain)
            
            if tech_data and tech_data.get("erp_system"):
                detected += 1
                break
        
        if tech_data:
            record.update(tech_data)
    
    log.info(f"Detected tech stack for {detected}/{len(records)} records")
    return records

# BuiltWith API
async def detect_via_builtwith(domain: str) -> dict:
    response = await http.get(
        "https://api.builtwith.com/v21/api.json",
        params={"KEY": os.getenv("BUILTWITH_API_KEY"), "LOOKUP": domain}
    )
    
    if response.status_code != 200:
        return None
    
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
        "tech_stack": technologies[:20],  # Limit
        "erp_system": erp_system,
        "crm_system": crm_system,
        "tech_source": "builtwith"
    }

# Job Posting Analysis
async def detect_via_job_postings(company_name: str) -> dict:
    """Detect ERP by analyzing job postings."""
    # Search Indeed for company jobs
    search_url = f"https://www.indeed.com/jobs?q={quote(company_name)}"
    response = await http.get(search_url)
    
    if response.status_code != 200:
        return None
    
    soup = BeautifulSoup(response.text, 'lxml')
    job_text = soup.get_text().lower()
    
    detected_erp = None
    max_count = 0
    
    for erp_key, keywords in ERP_KEYWORDS.items():
        for keyword in keywords:
            count = job_text.count(keyword.lower())
            if count > max_count:
                detected_erp = keyword
                max_count = count
    
    if max_count >= 2:  # At least 2 mentions
        return {
            "erp_system": detected_erp,
            "tech_source": "job_postings"
        }
    return None

# Website Fingerprinting
async def detect_via_fingerprint(domain: str) -> dict:
    """Detect ERP by analyzing website source."""
    try:
        response = await http.get(f"https://{domain}", timeout=10)
    except:
        return None
    
    html = response.text.lower()
    
    # Check for SAP
    if any(s in html for s in ["sap-ui-core.js", "sapui5", "/sap/"]):
        return {"erp_system": "SAP", "tech_source": "website_fingerprint"}
    
    # Check for Salesforce
    if "salesforce.com" in html or "force.com" in html:
        return {"crm_system": "Salesforce", "tech_source": "website_fingerprint"}
    
    return None
```

---

## Contact Finder Agent

### Purpose
Identify key decision-makers at target companies.

### Input
```json
{
  "records": [{"company_name": "Acme Inc.", "website": "https://acme.com"}],
  "target_titles": ["CIO", "VP IT", "IT Director", "COO", "CFO"]
}
```

### Output
```json
{
  "records": [{
    "company_name": "Acme Inc.",
    "contacts": [{
      "name": "John Smith",
      "title": "VP of IT",
      "email": "jsmith@acme.com",
      "phone": "+1-555-123-4567",
      "linkedin_url": "https://linkedin.com/in/johnsmith",
      "source": "apollo"
    }]
  }],
  "contacts_found": 156
}
```

### Target Titles by Priority

| Priority | Titles | Relevance |
|----------|--------|-----------|
| 1 (Primary) | CIO, VP IT, IT Director, ERP Manager | Direct ERP decision maker |
| 1 (Primary) | COO, VP Operations | Key operational stakeholder |
| 2 (Secondary) | CFO, Controller | Budget authority |
| 2 (Secondary) | CEO, President, Owner | Final approval |
| 3 (Tertiary) | Plant Manager, VP Manufacturing | End user champion |

### Process

```python
TARGET_TITLE_PATTERNS = [
    r"chief information officer|cio",
    r"vp?\s*(of\s*)?information technology|vp?\s*(of\s*)?it",
    r"it director|director.*it",
    r"erp manager",
    r"chief operating officer|coo",
    r"vp?\s*(of\s*)?operations",
    r"chief financial officer|cfo",
    r"controller",
    r"ceo|president|owner",
    r"plant manager",
]

async def find_contacts(records: list[dict], target_titles: list[str]) -> list[dict]:
    total_contacts = 0
    
    for record in records:
        domain = extract_domain(record.get("website"))
        company_name = record.get("company_name")
        
        contacts = []
        
        # Try Apollo
        if domain:
            apollo_contacts = await search_apollo_contacts(domain, target_titles)
            contacts.extend(apollo_contacts)
        
        # Try ZoomInfo as backup
        if len(contacts) < 2 and company_name:
            zi_contacts = await search_zoominfo_contacts(company_name, target_titles)
            contacts.extend(zi_contacts)
        
        # Deduplicate
        contacts = dedupe_contacts(contacts)
        
        # Filter to target titles
        contacts = [c for c in contacts if is_target_title(c.get("title", ""))]
        
        record["contacts"] = contacts[:5]  # Max 5 per company
        total_contacts += len(record["contacts"])
    
    log.info(f"Found {total_contacts} contacts across {len(records)} companies")
    return records

async def search_apollo_contacts(domain: str, target_titles: list[str]) -> list[dict]:
    response = await http.post(
        "https://api.apollo.io/v1/mixed_people/search",
        headers={"X-Api-Key": os.getenv("APOLLO_API_KEY")},
        json={
            "q_organization_domains": domain,
            "person_titles": target_titles,
            "page": 1,
            "per_page": 10
        }
    )
    
    if response.status_code != 200:
        return []
    
    contacts = []
    for person in response.json().get("people", []):
        contacts.append({
            "name": person.get("name"),
            "title": person.get("title"),
            "email": person.get("email"),
            "phone": person.get("phone_numbers", [{}])[0].get("number"),
            "linkedin_url": person.get("linkedin_url"),
            "source": "apollo"
        })
    
    return contacts

def is_target_title(title: str) -> bool:
    if not title:
        return False
    title_lower = title.lower()
    return any(re.search(pattern, title_lower) for pattern in TARGET_TITLE_PATTERNS)

def dedupe_contacts(contacts: list[dict]) -> list[dict]:
    seen = set()
    unique = []
    for c in contacts:
        key = c.get("email", "").lower() or c.get("name", "").lower()
        if key and key not in seen:
            seen.add(key)
            unique.append(c)
    return unique
```

---

## Rate Limits

| Provider | Rate | Daily Quota | Cost |
|----------|------|-------------|------|
| Clearbit | 10/sec | 10,000 | $0.05/lookup |
| ZoomInfo | 1.5/sec | 5,000 | $0.10/lookup |
| Apollo | 0.8/sec | 5,000 | $0.08/lookup |
| BuiltWith | 5/sec | 5,000 | $0.02/lookup |

---

## Output Format

Save to: `data/processed/{association}/enriched.jsonl`

```json
{
  "company_name": "Acme Inc.",
  "website": "https://acme.com",
  "employee_count_min": 201,
  "employee_count_max": 500,
  "revenue_min_usd": 50000000,
  "erp_system": "Epicor",
  "contacts": [{"name": "John Smith", "title": "VP IT", "email": "jsmith@acme.com"}],
  "firmographic_source": "clearbit",
  "tech_source": "builtwith"
}
```

---

## Best Practices

1. **Batch API calls** - Respect rate limits
2. **Cache responses** - APIs are expensive
3. **Use multiple providers** - Improve match rates
4. **Prioritize by value** - Enrich high-potential companies first
5. **Validate email** - Use Hunter.io or similar to verify
