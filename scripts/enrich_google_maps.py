#!/usr/bin/env python3
"""
Schema.org + Homepage Enrichment
NAM Intelligence Pipeline

Enriches company records by crawling company homepages and /contact pages for:
- Schema.org JSON-LD structured data (Organization, LocalBusiness)
- Address blocks (street, city, state, ZIP)
- Phone numbers
- Employee count estimates
- Manufacturing certifications (ISO 9001, AS9100, ITAR, etc.)
- Manufacturing capabilities (CNC, stamping, welding, etc.)
- Founded year
- Google Maps embed URLs

No API keys required. Uses httpx with stealth headers.
Rate limit: 1 request per 2 seconds (polite crawling).

Usage:
  python scripts/enrich_google_maps.py                   # All companies missing data
  python scripts/enrich_google_maps.py --limit 200       # First 200 missing data
  python scripts/enrich_google_maps.py --assoc NADCA     # Filter by association
  python scripts/enrich_google_maps.py --all             # Process all companies
"""

import argparse
import csv
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "exports" / "companies_all.csv"
OUTPUT_PATH = BASE_DIR / "data" / "processed" / "gmaps_enriched.jsonl"

# ── HTTP Config ────────────────────────────────────────────────────────────────

STEALTH_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

RATE_LIMIT_DELAY = 2.0  # seconds between requests

# ── Contact pages to try ───────────────────────────────────────────────────────

CONTACT_PATHS = [
    "/contact",
    "/contact-us",
    "/contact-us/",
    "/contact/",
    "/about",
    "/about-us",
    "/about/",
    "/about-us/",
]

# ── Regex patterns ─────────────────────────────────────────────────────────────

# US phone: (xxx) xxx-xxxx  xxx-xxx-xxxx  xxx.xxx.xxxx  +1-xxx-xxx-xxxx
PHONE_RE = re.compile(
    r'(?:(?:\+1[-.\s]?)?'
    r'(?:\(?\d{3}\)?[-.\s]?)\d{3}[-.\s]\d{4})',
    re.VERBOSE,
)

# US street address: 1234 Main St, Suite 100
STREET_RE = re.compile(
    r'\b(\d{1,5})\s+'
    r'([A-Z][a-zA-Z0-9\s]{2,40}?)'
    r'\s+(St|Street|Ave|Avenue|Blvd|Boulevard|Dr|Drive|Rd|Road|Ln|Lane|'
    r'Way|Pkwy|Parkway|Ct|Court|Pl|Place|Cir|Circle|Hwy|Highway|'
    r'Suite|Ste|Bldg|Building|Floor|Fl)\b',
    re.IGNORECASE,
)

# ZIP code (5-digit or 5+4)
ZIP_RE = re.compile(r'\b(\d{5})(?:-\d{4})?\b')

# US state abbreviations in address context
STATE_RE = re.compile(
    r'\b(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|'
    r'MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|'
    r'TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)\b'
)

# City, State ZIP pattern
CITY_STATE_ZIP_RE = re.compile(
    r'([A-Z][a-zA-Z\s]{2,30}),\s*'
    r'(AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|'
    r'MA|MI|MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|'
    r'TN|TX|UT|VT|VA|WA|WV|WI|WY|DC)'
    r'(?:\s+(\d{5}(?:-\d{4})?))?',
    re.IGNORECASE,
)

# Employee count patterns
EMPLOYEE_RE = re.compile(
    r'(?:team of|employs?|over|more than|approximately|about|nearly|'
    r'workforce of|staff of|employees?:?)\s*'
    r'(\d[\d,]*)\s*(?:\+\s*)?(?:employees?|people|staff|team members?|professionals?)?',
    re.IGNORECASE,
)

# Year founded
FOUNDED_RE = re.compile(
    r'(?:founded|established|since|incorporated|started|began)\s+(?:in\s+)?(\d{4})',
    re.IGNORECASE,
)

# Google Maps embed URL
GMAPS_EMBED_RE = re.compile(
    r'(?:maps\.google\.com|google\.com/maps|maps\.googleapis\.com)'
    r'[^\s"\'<>]*',
    re.IGNORECASE,
)

# ── Manufacturing certifications ───────────────────────────────────────────────

CERTIFICATION_PATTERNS = [
    (re.compile(r'\bISO\s*9001\b', re.IGNORECASE), "ISO 9001"),
    (re.compile(r'\bISO\s*14001\b', re.IGNORECASE), "ISO 14001"),
    (re.compile(r'\bISO\s*45001\b', re.IGNORECASE), "ISO 45001"),
    (re.compile(r'\bISO\s*13485\b', re.IGNORECASE), "ISO 13485"),
    (re.compile(r'\bISO\s*27001\b', re.IGNORECASE), "ISO 27001"),
    (re.compile(r'\bAS\s*9100\b', re.IGNORECASE), "AS9100"),
    (re.compile(r'\bAS\s*9120\b', re.IGNORECASE), "AS9120"),
    (re.compile(r'\bITAR\b', re.IGNORECASE), "ITAR"),
    (re.compile(r'\bNADCAP\b', re.IGNORECASE), "NADCAP"),
    (re.compile(r'\bTS\s*16949\b', re.IGNORECASE), "IATF 16949"),
    (re.compile(r'\bIATF\s*16949\b', re.IGNORECASE), "IATF 16949"),
    (re.compile(r'\bAPI\s*Q1\b', re.IGNORECASE), "API Q1"),
    (re.compile(r'\bAPI\s*Q2\b', re.IGNORECASE), "API Q2"),
    (re.compile(r'\bUL\s+Listed\b', re.IGNORECASE), "UL Listed"),
    (re.compile(r'\bNIST\b', re.IGNORECASE), "NIST"),
    (re.compile(r'\bCMMI\b', re.IGNORECASE), "CMMI"),
    (re.compile(r'\bMIL-SPEC\b', re.IGNORECASE), "MIL-SPEC"),
    (re.compile(r'\bRoHS\b', re.IGNORECASE), "RoHS"),
    (re.compile(r'\bREACH\b', re.IGNORECASE), "REACH"),
    (re.compile(r'\bCE\s+[Mm]arking\b', re.IGNORECASE), "CE Marking"),
    (re.compile(r'\bFDA\s+[Rr]egistered\b', re.IGNORECASE), "FDA Registered"),
    (re.compile(r'\bA2LA\b', re.IGNORECASE), "A2LA"),
]

# ── Manufacturing capabilities ─────────────────────────────────────────────────

CAPABILITY_PATTERNS = [
    (re.compile(r'\bCNC\s+(?:machining|milling|turning|grinding|routing)\b', re.IGNORECASE), "CNC Machining"),
    (re.compile(r'\bCNC\b', re.IGNORECASE), "CNC"),
    (re.compile(r'\b(?:metal\s+)?stamping\b', re.IGNORECASE), "Metal Stamping"),
    (re.compile(r'\b(?:metal\s+)?fabricat(?:ion|ing)\b', re.IGNORECASE), "Metal Fabrication"),
    (re.compile(r'\bsheet\s+metal\b', re.IGNORECASE), "Sheet Metal"),
    (re.compile(r'\bdie\s+cast(?:ing)?\b', re.IGNORECASE), "Die Casting"),
    (re.compile(r'\binjection\s+mold(?:ing)?\b', re.IGNORECASE), "Injection Molding"),
    (re.compile(r'\bplastic\s+(?:injection|mold(?:ing)?)\b', re.IGNORECASE), "Plastic Molding"),
    (re.compile(r'\bweld(?:ing)?\b', re.IGNORECASE), "Welding"),
    (re.compile(r'\bpunch(?:ing)?\s+press\b', re.IGNORECASE), "Punch Press"),
    (re.compile(r'\bprogressive\s+die\b', re.IGNORECASE), "Progressive Die"),
    (re.compile(r'\bheat\s+treat(?:ment|ing)?\b', re.IGNORECASE), "Heat Treatment"),
    (re.compile(r'\bpowder\s+coat(?:ing)?\b', re.IGNORECASE), "Powder Coating"),
    (re.compile(r'\belectroplat(?:ing|e)\b', re.IGNORECASE), "Electroplating"),
    (re.compile(r'\banodiz(?:ing|e)\b', re.IGNORECASE), "Anodizing"),
    (re.compile(r'\bforging?\b', re.IGNORECASE), "Forging"),
    (re.compile(r'\bcasting\b', re.IGNORECASE), "Casting"),
    (re.compile(r'\bextrusion\b', re.IGNORECASE), "Extrusion"),
    (re.compile(r'\bturning\b', re.IGNORECASE), "Turning"),
    (re.compile(r'\bmilling\b', re.IGNORECASE), "Milling"),
    (re.compile(r'\bgrinding\b', re.IGNORECASE), "Grinding"),
    (re.compile(r'\bEDM\b', re.IGNORECASE), "EDM"),
    (re.compile(r'\blaser\s+cut(?:ting)?\b', re.IGNORECASE), "Laser Cutting"),
    (re.compile(r'\bwaterjet\b', re.IGNORECASE), "Waterjet Cutting"),
    (re.compile(r'\bplasma\s+cut(?:ting)?\b', re.IGNORECASE), "Plasma Cutting"),
    (re.compile(r'\b3D\s+print(?:ing)?\b', re.IGNORECASE), "3D Printing"),
    (re.compile(r'\badditive\s+manufacturing\b', re.IGNORECASE), "Additive Manufacturing"),
    (re.compile(r'\bassembly\b', re.IGNORECASE), "Assembly"),
    (re.compile(r'\bprototyp(?:ing|e)\b', re.IGNORECASE), "Prototyping"),
    (re.compile(r'\btool(?:ing|s?\s+and\s+die)\b', re.IGNORECASE), "Tooling"),
    (re.compile(r'\bscrew\s+machine(?:\s+products?)?\b', re.IGNORECASE), "Screw Machine"),
    (re.compile(r'\bbroaching\b', re.IGNORECASE), "Broaching"),
    (re.compile(r'\bhoning\b', re.IGNORECASE), "Honing"),
    (re.compile(r'\blast(?:ing)?\b', re.IGNORECASE), "Blasting"),
    (re.compile(r'\bdeburr(?:ing)?\b', re.IGNORECASE), "Deburring"),
]


# ── Schema.org extraction ──────────────────────────────────────────────────────

def extract_schema_org(html: str) -> dict:
    """Extract Organization / LocalBusiness data from schema.org JSON-LD blocks."""
    result = {}

    # Find all <script type="application/ld+json"> blocks
    ld_blocks = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html,
        re.IGNORECASE | re.DOTALL,
    )

    for raw in ld_blocks:
        raw = raw.strip()
        if not raw:
            continue

        try:
            data = json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            # Try to salvage common truncation issues
            try:
                # Strip trailing garbage and try again
                cleaned = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f]', '', raw)
                data = json.loads(cleaned)
            except Exception:
                continue

        # Handle @graph arrays
        nodes = []
        if isinstance(data, list):
            nodes = data
        elif isinstance(data, dict):
            if "@graph" in data:
                nodes = data["@graph"]
            else:
                nodes = [data]

        for node in nodes:
            if not isinstance(node, dict):
                continue

            schema_type = node.get("@type", "")
            if isinstance(schema_type, list):
                schema_type = schema_type[0] if schema_type else ""

            target_types = (
                "Organization", "LocalBusiness", "Corporation",
                "MedicalOrganization", "SportsOrganization",
                "AutomotiveBusiness", "Store",
            )
            if not any(t in schema_type for t in target_types):
                continue

            # Record schema type
            if schema_type and not result.get("schema_org_type"):
                result["schema_org_type"] = schema_type

            # Telephone
            if node.get("telephone") and not result.get("phone"):
                result["phone"] = str(node["telephone"]).strip()

            # URL
            if node.get("url") and not result.get("schema_url"):
                result["schema_url"] = str(node["url"]).strip()

            # Founded year
            if node.get("foundingDate") and not result.get("founded_year"):
                year_str = str(node["foundingDate"])
                year_match = re.search(r'\b(19|20)\d{2}\b', year_str)
                if year_match:
                    result["founded_year"] = int(year_match.group())

            # Number of employees
            if node.get("numberOfEmployees") and not result.get("employee_estimate"):
                emp = node["numberOfEmployees"]
                if isinstance(emp, dict):
                    # QuantitativeValue: {"@type": "QuantitativeValue", "value": 250}
                    val = emp.get("value") or emp.get("maxValue") or emp.get("minValue")
                    if val:
                        try:
                            result["employee_estimate"] = int(str(val).replace(",", ""))
                        except (ValueError, TypeError):
                            pass
                elif isinstance(emp, (int, float)):
                    result["employee_estimate"] = int(emp)
                elif isinstance(emp, str):
                    try:
                        result["employee_estimate"] = int(emp.replace(",", ""))
                    except (ValueError, TypeError):
                        pass

            # Description
            if node.get("description") and not result.get("schema_description"):
                result["schema_description"] = str(node["description"])[:300]

            # Address
            addr = node.get("address")
            if addr and not result.get("street_address"):
                if isinstance(addr, str):
                    result["street_address"] = addr
                elif isinstance(addr, dict):
                    parts = []
                    street = addr.get("streetAddress", "")
                    if street:
                        parts.append(street)
                        result["street_address"] = street
                    city = addr.get("addressLocality", "")
                    if city and not result.get("city"):
                        result["city"] = city
                    state = addr.get("addressRegion", "")
                    if state and not result.get("state"):
                        result["state"] = state.upper()[:2] if len(state) > 2 else state.upper()
                    zipcode = addr.get("postalCode", "")
                    if zipcode and not result.get("zip_code"):
                        result["zip_code"] = str(zipcode)
                    country = addr.get("addressCountry", "")
                    if country and not result.get("country"):
                        result["country"] = country

            # Geo coordinates
            geo = node.get("geo")
            if geo and isinstance(geo, dict):
                lat = geo.get("latitude") or geo.get("lat")
                lng = geo.get("longitude") or geo.get("lng") or geo.get("long")
                if lat and lng and not result.get("latitude"):
                    try:
                        result["latitude"] = float(lat)
                        result["longitude"] = float(lng)
                    except (ValueError, TypeError):
                        pass

            # hasMap / maps
            has_map = node.get("hasMap") or node.get("map")
            if has_map and not result.get("google_maps_url"):
                maps_url = str(has_map)
                if "google" in maps_url.lower() or "maps" in maps_url.lower():
                    result["google_maps_url"] = maps_url

            # Social media profiles
            same_as = node.get("sameAs", [])
            if isinstance(same_as, str):
                same_as = [same_as]
            if same_as and not result.get("linkedin_url"):
                for url in same_as:
                    url_lower = url.lower()
                    if "linkedin.com/company" in url_lower:
                        result["linkedin_url"] = url
                        break

    return result


# ── HTML pattern extraction ────────────────────────────────────────────────────

def extract_address_from_html(html: str, text: str) -> dict:
    """Extract address components from raw HTML and visible text."""
    result = {}

    # 1. Look for structured address HTML patterns
    #    <address> tag content
    address_tags = re.findall(r'<address[^>]*>(.*?)</address>', html, re.IGNORECASE | re.DOTALL)
    for addr_html in address_tags:
        addr_text = re.sub(r'<[^>]+>', ' ', addr_html)
        addr_text = re.sub(r'\s+', ' ', addr_text).strip()
        if addr_text and len(addr_text) > 10:
            # Try to parse City, State ZIP from address text
            m = CITY_STATE_ZIP_RE.search(addr_text)
            if m:
                if not result.get("city"):
                    result["city"] = m.group(1).strip().title()
                if not result.get("state"):
                    result["state"] = m.group(2).upper()
                if m.group(3) and not result.get("zip_code"):
                    result["zip_code"] = m.group(3)

            # Street address from address tag
            sm = STREET_RE.search(addr_text)
            if sm and not result.get("street_address"):
                # Take a bit more context around the match
                start = max(0, sm.start() - 2)
                end = min(len(addr_text), sm.end() + 30)
                raw_street = addr_text[start:end].strip()
                # Truncate at newline or common delimiters
                raw_street = re.split(r'[|\n\r]', raw_street)[0].strip()
                result["street_address"] = raw_street[:100]
            break  # Use first <address> tag only

    # 2. City, State ZIP from visible text (if not found above)
    if not result.get("city"):
        matches = CITY_STATE_ZIP_RE.findall(text)
        for match in matches:
            city, state, zipcode = match
            city = city.strip()
            # Filter noise: skip very long "cities" or common false positives
            if 2 <= len(city) <= 30 and not re.search(r'\d', city):
                result["city"] = city.title()
                result["state"] = state.upper()
                if zipcode:
                    result["zip_code"] = zipcode
                break

    # 3. ZIP code alone (if city not found)
    if not result.get("zip_code"):
        zm = ZIP_RE.search(text)
        if zm:
            result["zip_code"] = zm.group(1)

    return result


def extract_phone_from_html(text: str) -> str:
    """Extract first US phone number from text."""
    phones = PHONE_RE.findall(text)
    # Filter out very short matches and obvious non-phones (years, IDs)
    for phone in phones:
        phone = phone.strip()
        digits_only = re.sub(r'\D', '', phone)
        if len(digits_only) in (10, 11):
            return phone
    return ""


def extract_employee_estimate(text: str) -> int | None:
    """Extract employee count estimate from text."""
    for match in EMPLOYEE_RE.finditer(text):
        try:
            count = int(match.group(1).replace(",", ""))
            # Sanity check: 1 to 500,000
            if 1 <= count <= 500_000:
                return count
        except (ValueError, IndexError):
            continue
    return None


def extract_founded_year(text: str) -> int | None:
    """Extract founding year from text."""
    for match in FOUNDED_RE.finditer(text):
        try:
            year = int(match.group(1))
            if 1800 <= year <= 2024:
                return year
        except (ValueError, IndexError):
            continue
    return None


def extract_certifications(text: str) -> list[str]:
    """Extract manufacturing certifications mentioned in text."""
    found = []
    for pattern, cert_name in CERTIFICATION_PATTERNS:
        if pattern.search(text) and cert_name not in found:
            found.append(cert_name)
    return found


def extract_capabilities(text: str) -> list[str]:
    """Extract manufacturing capabilities mentioned in text."""
    found = []
    for pattern, cap_name in CAPABILITY_PATTERNS:
        if pattern.search(text) and cap_name not in found:
            found.append(cap_name)
    # Deduplicate: if "CNC Machining" found, remove bare "CNC"
    if "CNC Machining" in found and "CNC" in found:
        found.remove("CNC")
    return found


def extract_google_maps_embed(html: str) -> str:
    """Find Google Maps embed URL in page HTML."""
    # iframe src with google maps
    iframe_re = re.compile(
        r'<iframe[^>]+src=["\']([^"\']*(?:maps\.google|google\.com/maps|maps\.googleapis)[^"\']*)["\']',
        re.IGNORECASE,
    )
    m = iframe_re.search(html)
    if m:
        return m.group(1)

    # data-src lazy loaded
    data_src_re = re.compile(
        r'data-src=["\']([^"\']*(?:maps\.google|google\.com/maps)[^"\']*)["\']',
        re.IGNORECASE,
    )
    m = data_src_re.search(html)
    if m:
        return m.group(1)

    return ""


# ── HTTP fetching ──────────────────────────────────────────────────────────────

def fetch_page(client, url: str, timeout: int = 12) -> tuple[str, str]:
    """Fetch a URL and return (html, final_url). Returns ('', '') on failure."""
    try:
        resp = client.get(url, timeout=timeout)
        if resp.status_code == 200:
            return resp.text, str(resp.url)
        return "", ""
    except Exception:
        return "", ""


def normalize_website(website: str) -> str:
    """Ensure website has https:// prefix."""
    website = website.strip()
    if not website:
        return ""
    if "://" not in website:
        return f"https://{website}"
    return website


# ── Data loading ───────────────────────────────────────────────────────────────

def load_companies(limit: int | None = None, assoc: str | None = None, all_records: bool = False) -> list[dict]:
    """
    Load companies from companies_all.csv.

    Priority order:
    1. Missing phone AND address (most valuable to enrich)
    2. Missing phone only
    3. Missing address only
    4. Complete (skipped unless --all flag passed)
    """
    if not CSV_PATH.exists():
        print(f"ERROR: {CSV_PATH} not found")
        sys.exit(1)

    records = []
    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Must have a website to crawl
            website = (row.get("website") or "").strip()
            domain = (row.get("domain") or "").strip()
            if not website and not domain:
                continue

            # Filter by association if specified
            if assoc:
                rec_assoc = (row.get("associations") or "").upper()
                if assoc.upper() not in rec_assoc:
                    continue

            has_phone = bool((row.get("phone") or "").strip())
            has_street = bool((row.get("street") or "").strip())
            has_zip = bool((row.get("zip_code") or "").strip())
            has_address = has_street or has_zip

            # Skip complete records unless --all
            if has_phone and has_address and not all_records:
                continue

            # Priority score: lower = higher priority
            # 0 = missing both, 1 = missing phone, 2 = missing address, 3 = has both
            if not has_phone and not has_address:
                priority = 0
            elif not has_phone:
                priority = 1
            elif not has_address:
                priority = 2
            else:
                priority = 3

            records.append({
                "company_name": (row.get("company_name") or "").strip(),
                "website": normalize_website(website or domain),
                "domain": domain,
                "associations": (row.get("associations") or "").strip(),
                "city": (row.get("city") or "").strip(),
                "state": (row.get("state") or "").strip(),
                "existing_phone": (row.get("phone") or "").strip(),
                "existing_street": (row.get("street") or "").strip(),
                "existing_zip": (row.get("zip_code") or "").strip(),
                "_priority": priority,
            })

    # Sort by priority (missing most data first)
    records.sort(key=lambda r: r["_priority"])

    if limit:
        records = records[:limit]

    return records


# ── Core enrichment ────────────────────────────────────────────────────────────

def enrich_company(client, company: dict) -> dict:
    """
    Crawl a company's homepage and contact page, extract all available data.
    Returns a dict with all enriched fields.
    """
    base_url = company["website"]
    result = {
        "company_name": company["company_name"],
        "domain": company["domain"],
        "website": base_url,
        "associations": company["associations"],
        # Fields we're enriching
        "street_address": "",
        "city": "",
        "state": "",
        "zip_code": "",
        "phone": "",
        "employee_estimate": None,
        "certifications": [],
        "manufacturing_capabilities": [],
        "founded_year": None,
        "google_maps_url": "",
        "schema_org_type": "",
        "schema_description": "",
        "linkedin_url": "",
        "latitude": None,
        "longitude": None,
        # Metadata
        "pages_crawled": [],
        "enriched_at": datetime.now(timezone.utc).isoformat(),
        "enrichment_source": [],
    }

    all_text = ""
    all_html = ""

    # --- Step 1: Fetch homepage ---
    homepage_html, final_url = fetch_page(client, base_url)
    if homepage_html:
        result["pages_crawled"].append(final_url or base_url)
        all_html += homepage_html
        # Get visible text (strip tags)
        visible_text = re.sub(r'<[^>]+>', ' ', homepage_html)
        visible_text = re.sub(r'\s+', ' ', visible_text)
        all_text += visible_text

        # Schema.org from homepage
        schema_data = extract_schema_org(homepage_html)
        if schema_data:
            result["enrichment_source"].append("schema.org:homepage")
            for k, v in schema_data.items():
                if v and not result.get(k):
                    result[k] = v

        # Google Maps embed from homepage
        if not result["google_maps_url"]:
            gmaps_url = extract_google_maps_embed(homepage_html)
            if gmaps_url:
                result["google_maps_url"] = gmaps_url
                result["enrichment_source"].append("gmaps_embed:homepage")

    time.sleep(RATE_LIMIT_DELAY)

    # --- Step 2: Fetch /contact or /about page ---
    contact_html = ""
    for path in CONTACT_PATHS:
        contact_url = urljoin(base_url, path)
        # Avoid re-fetching the same URL as homepage
        if contact_url.rstrip("/") == base_url.rstrip("/"):
            continue

        chtml, cfinal = fetch_page(client, contact_url)
        if chtml and len(chtml) > 500:
            result["pages_crawled"].append(cfinal or contact_url)
            contact_html = chtml
            visible = re.sub(r'<[^>]+>', ' ', chtml)
            visible = re.sub(r'\s+', ' ', visible)
            all_text += " " + visible
            all_html += chtml

            # Schema.org from contact page
            contact_schema = extract_schema_org(chtml)
            if contact_schema:
                result["enrichment_source"].append(f"schema.org:{path}")
                for k, v in contact_schema.items():
                    if v and not result.get(k):
                        result[k] = v

            # Google Maps embed from contact page
            if not result["google_maps_url"]:
                gmaps_url = extract_google_maps_embed(chtml)
                if gmaps_url:
                    result["google_maps_url"] = gmaps_url
                    result["enrichment_source"].append(f"gmaps_embed:{path}")

            time.sleep(RATE_LIMIT_DELAY)
            break  # Found a contact page — stop checking more paths

    # --- Step 3: Pattern extraction from all collected text ---

    # Phone (skip if already found via schema.org)
    if not result["phone"]:
        phone = extract_phone_from_html(all_text)
        if phone:
            result["phone"] = phone
            result["enrichment_source"].append("html:phone_pattern")

    # Address (fill in gaps)
    addr_data = extract_address_from_html(all_html, all_text)
    for field in ("street_address", "city", "state", "zip_code"):
        if addr_data.get(field) and not result.get(field):
            result[field] = addr_data[field]
            result["enrichment_source"].append(f"html:{field}_pattern")

    # Preserve existing CSV values if we didn't find new ones
    if not result["city"] and company.get("city"):
        result["city"] = company["city"]
    if not result["state"] and company.get("state"):
        result["state"] = company["state"]
    if not result["phone"] and company.get("existing_phone"):
        result["phone"] = company["existing_phone"]
    if not result["street_address"] and company.get("existing_street"):
        result["street_address"] = company["existing_street"]
    if not result["zip_code"] and company.get("existing_zip"):
        result["zip_code"] = company["existing_zip"]

    # Employee estimate
    if result["employee_estimate"] is None:
        emp = extract_employee_estimate(all_text)
        if emp:
            result["employee_estimate"] = emp
            result["enrichment_source"].append("html:employee_pattern")

    # Founded year
    if result["founded_year"] is None:
        year = extract_founded_year(all_text)
        if year:
            result["founded_year"] = year
            result["enrichment_source"].append("html:founded_pattern")

    # Certifications (always accumulate from all pages)
    certs = extract_certifications(all_text)
    result["certifications"] = sorted(set(result.get("certifications", []) + certs))
    if certs:
        result["enrichment_source"].append("html:certifications")

    # Capabilities
    caps = extract_capabilities(all_text)
    result["manufacturing_capabilities"] = sorted(set(result.get("manufacturing_capabilities", []) + caps))
    if caps:
        result["enrichment_source"].append("html:capabilities")

    # Deduplicate enrichment_source
    result["enrichment_source"] = sorted(set(result["enrichment_source"]))

    return result


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Enrich company records via schema.org + homepage crawling (no API keys required)"
    )
    parser.add_argument("--limit", type=int, default=None, help="Max companies to process")
    parser.add_argument("--assoc", type=str, default=None, help="Filter by association code (e.g. NADCA, PMA)")
    parser.add_argument("--all", action="store_true", dest="all_records",
                        help="Process all companies (default: only those missing phone/address)")
    args = parser.parse_args()

    print("=" * 65)
    print("NAM Intelligence Pipeline - Schema.org + Homepage Enrichment")
    print("=" * 65)

    companies = load_companies(
        limit=args.limit,
        assoc=args.assoc,
        all_records=args.all_records,
    )

    missing_both = sum(1 for c in companies if c["_priority"] == 0)
    missing_phone = sum(1 for c in companies if c["_priority"] == 1)
    missing_addr = sum(1 for c in companies if c["_priority"] == 2)
    has_both = sum(1 for c in companies if c["_priority"] == 3)

    print(f"\nCompanies to process: {len(companies)}")
    print(f"  Missing phone + address: {missing_both}")
    print(f"  Missing phone only:      {missing_phone}")
    print(f"  Missing address only:    {missing_addr}")
    if has_both:
        print(f"  Already complete:        {has_both}")
    print(f"\nOutput: {OUTPUT_PATH}")
    print(f"Rate limit: {RATE_LIMIT_DELAY}s between requests")
    print()

    import httpx

    client = httpx.Client(
        headers=STEALTH_HEADERS,
        follow_redirects=True,
        timeout=12,
    )

    results = []
    phone_found = 0
    addr_found = 0
    cert_found = 0
    cap_found = 0
    schema_found = 0
    errors = 0

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(OUTPUT_PATH, "w", encoding="utf-8") as outf:
            for i, company in enumerate(companies):
                company_name = company["company_name"]

                try:
                    enriched = enrich_company(client, company)
                    results.append(enriched)
                    outf.write(json.dumps(enriched, ensure_ascii=False) + "\n")
                    outf.flush()

                    # Track stats
                    if enriched.get("phone") and not company.get("existing_phone"):
                        phone_found += 1
                    if enriched.get("zip_code") and not company.get("existing_zip"):
                        addr_found += 1
                    if enriched.get("certifications"):
                        cert_found += 1
                    if enriched.get("manufacturing_capabilities"):
                        cap_found += 1
                    if enriched.get("schema_org_type") or enriched.get("street_address"):
                        schema_found += 1

                except KeyboardInterrupt:
                    print("\nInterrupted by user — partial results saved.")
                    break
                except Exception as e:
                    errors += 1
                    # Still write a minimal record so we know we processed it
                    failed = {
                        "company_name": company_name,
                        "domain": company.get("domain", ""),
                        "enriched_at": datetime.now(timezone.utc).isoformat(),
                        "error": str(e)[:200],
                    }
                    results.append(failed)
                    outf.write(json.dumps(failed, ensure_ascii=False) + "\n")
                    outf.flush()

                # Progress every 25 companies
                if (i + 1) % 25 == 0:
                    pct = (i + 1) / len(companies) * 100
                    print(
                        f"  [{i+1:>4}/{len(companies)}] {pct:4.0f}%  "
                        f"Phone: {phone_found}  Addr: {addr_found}  "
                        f"Certs: {cert_found}  Caps: {cap_found}  "
                        f"Schema: {schema_found}  Errors: {errors}"
                    )

    finally:
        client.close()

    # ── Final stats ────────────────────────────────────────────────────────────
    total = len(results)
    print(f"\n{'=' * 65}")
    print(f"Enrichment Results")
    print(f"{'=' * 65}")
    print(f"  Companies processed:    {total}")
    print(f"  New phone numbers:      {phone_found} ({phone_found/max(total,1)*100:.1f}%)")
    print(f"  New addresses found:    {addr_found} ({addr_found/max(total,1)*100:.1f}%)")
    print(f"  Certifications found:   {cert_found} ({cert_found/max(total,1)*100:.1f}%)")
    print(f"  Capabilities found:     {cap_found} ({cap_found/max(total,1)*100:.1f}%)")
    print(f"  Schema.org hits:        {schema_found} ({schema_found/max(total,1)*100:.1f}%)")
    print(f"  Errors:                 {errors}")
    print(f"\n  Output: {OUTPUT_PATH}")

    # Certification breakdown
    from collections import Counter
    all_certs: list[str] = []
    all_caps: list[str] = []
    for rec in results:
        all_certs.extend(rec.get("certifications") or [])
        all_caps.extend(rec.get("manufacturing_capabilities") or [])

    if all_certs:
        print(f"\n  Top Certifications:")
        for cert, count in Counter(all_certs).most_common(15):
            print(f"    {cert}: {count}")

    if all_caps:
        print(f"\n  Top Manufacturing Capabilities:")
        for cap, count in Counter(all_caps).most_common(15):
            print(f"    {cap}: {count}")

    print("\nDone!")


if __name__ == "__main__":
    main()
