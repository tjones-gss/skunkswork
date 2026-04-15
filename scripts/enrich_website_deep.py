#!/usr/bin/env python3
"""
Deep Website Enrichment Script
NAM Intelligence Pipeline

For each company website, crawls multiple pages and extracts:
  1. Executive team contacts (name + title + email) from /about, /team, /leadership
  2. Certifications mentioned anywhere (ISO 9001, AS9100, ITAR, NADCAP, etc.)
  3. Manufacturing capabilities (CNC, injection molding, die casting, etc.)
  4. Company description and tagline from homepage meta tags / hero text
  5. Founded year from text patterns

Crawl strategy per company:
  - Homepage: meta description, title, schema.org, hero text, outbound links to team pages
  - Best team/about/leadership page found via link scan
  - /contact page (phone, email, address)
  - One additional discovery page if budget allows

Rate limits:
  - 1.5s between requests within a company
  - 3.0s between companies

Usage:
  python scripts/enrich_website_deep.py                    # First 200 companies
  python scripts/enrich_website_deep.py --limit 50         # First 50
  python scripts/enrich_website_deep.py --assoc AGMA       # Filter by association
  python scripts/enrich_website_deep.py --resume           # Skip already-crawled domains

Output: data/processed/website_deep_enriched.jsonl
"""

import argparse
import csv
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin, urlparse

import httpx

try:
    from bs4 import BeautifulSoup
except ImportError:
    print("ERROR: beautifulsoup4 is required. Run: pip install beautifulsoup4")
    raise SystemExit(1)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "exports" / "companies_all.csv"
OUTPUT_PATH = BASE_DIR / "data" / "processed" / "website_deep_enriched.jsonl"

# ---------------------------------------------------------------------------
# HTTP client config
# ---------------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
]

STEALTH_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "DNT": "1",
}

DELAY_BETWEEN_REQUESTS = 1.5   # seconds within a company
DELAY_BETWEEN_COMPANIES = 3.0  # seconds between companies
MAX_PAGES_PER_COMPANY = 4
REQUEST_TIMEOUT = 15

# ---------------------------------------------------------------------------
# Decision-maker titles (ERP buying committee)
# ---------------------------------------------------------------------------
TARGET_TITLES = [
    "ceo", "chief executive officer", "chief executive",
    "president",
    "owner", "co-owner", "co-founder", "founder",
    "managing director", "managing partner",
    "cfo", "chief financial officer",
    "vp finance", "vice president finance", "vice president of finance",
    "controller", "vp of finance",
    "coo", "chief operating officer",
    "vp operations", "vice president operations", "vice president of operations",
    "director of operations", "operations director",
    "plant manager", "plant director",
    "general manager",
    "cio", "chief information officer",
    "cto", "chief technology officer",
    "vp it", "vp of it", "vice president it", "vice president of it",
    "it director", "director of it", "director, it",
    "mis director", "is director",
    "vp manufacturing", "vice president manufacturing", "vice president of manufacturing",
    "director of manufacturing",
    "vp engineering", "vice president engineering", "vice president of engineering",
    "director of engineering", "engineering director",
    "vp supply chain", "vice president supply chain",
    "purchasing director", "director of purchasing",
    "erp manager", "erp director", "erp administrator",
]

# ---------------------------------------------------------------------------
# Certification keywords (case-insensitive)
# ---------------------------------------------------------------------------
CERT_PATTERNS = [
    (re.compile(r'\bISO\s*9001\b', re.IGNORECASE), "ISO 9001"),
    (re.compile(r'\bISO\s*14001\b', re.IGNORECASE), "ISO 14001"),
    (re.compile(r'\bISO\s*13485\b', re.IGNORECASE), "ISO 13485"),
    (re.compile(r'\bISO\s*27001\b', re.IGNORECASE), "ISO 27001"),
    (re.compile(r'\bISO\s*45001\b', re.IGNORECASE), "ISO 45001"),
    (re.compile(r'\bAS\s*9100\b', re.IGNORECASE), "AS9100"),
    (re.compile(r'\bAS\s*9120\b', re.IGNORECASE), "AS9120"),
    (re.compile(r'\bAS\s*9003\b', re.IGNORECASE), "AS9003"),
    (re.compile(r'\bITAR\b', re.IGNORECASE), "ITAR"),
    (re.compile(r'\bNADCAP\b', re.IGNORECASE), "NADCAP"),
    (re.compile(r'\bNIST\s*800[-\s]?171\b', re.IGNORECASE), "NIST 800-171"),
    (re.compile(r'\bCMMC\b', re.IGNORECASE), "CMMC"),
    (re.compile(r'\bUL\s+[Ll]isted\b', re.IGNORECASE), "UL Listed"),
    (re.compile(r'\bCSA\s+[Cc]ertified\b', re.IGNORECASE), "CSA Certified"),
    (re.compile(r'\bFDA\s+[Rr]egistered\b', re.IGNORECASE), "FDA Registered"),
    (re.compile(r'\bRoHS\b', re.IGNORECASE), "RoHS"),
    (re.compile(r'\bREACH\b'), "REACH"),
    (re.compile(r'\bISO\s*TS\s*16949\b', re.IGNORECASE), "IATF 16949"),
    (re.compile(r'\bIATF\s*16949\b', re.IGNORECASE), "IATF 16949"),
    (re.compile(r'\bQS\s*9000\b', re.IGNORECASE), "QS-9000"),
]

# ---------------------------------------------------------------------------
# Manufacturing capability keywords
# ---------------------------------------------------------------------------
CAPABILITY_PATTERNS = [
    # CNC
    (re.compile(r'\bCNC\s+machining\b', re.IGNORECASE), "CNC machining"),
    (re.compile(r'\bCNC\s+turning\b', re.IGNORECASE), "CNC turning"),
    (re.compile(r'\bCNC\s+milling\b', re.IGNORECASE), "CNC milling"),
    (re.compile(r'\bCNC\s+lathe\b', re.IGNORECASE), "CNC lathe"),
    (re.compile(r'\b5[-\s]?axis\b', re.IGNORECASE), "5-axis machining"),
    (re.compile(r'\bprecision\s+machining\b', re.IGNORECASE), "precision machining"),
    (re.compile(r'\bscrew\s+machining\b', re.IGNORECASE), "screw machining"),
    # Molding
    (re.compile(r'\binjection\s+molding\b', re.IGNORECASE), "injection molding"),
    (re.compile(r'\bblow\s+molding\b', re.IGNORECASE), "blow molding"),
    (re.compile(r'\brotational\s+molding\b', re.IGNORECASE), "rotational molding"),
    (re.compile(r'\bthermoforming\b', re.IGNORECASE), "thermoforming"),
    (re.compile(r'\bcompression\s+molding\b', re.IGNORECASE), "compression molding"),
    # Casting
    (re.compile(r'\bdie\s+casting\b', re.IGNORECASE), "die casting"),
    (re.compile(r'\binvestment\s+casting\b', re.IGNORECASE), "investment casting"),
    (re.compile(r'\bsand\s+casting\b', re.IGNORECASE), "sand casting"),
    (re.compile(r'\bpermanent\s+mold\b', re.IGNORECASE), "permanent mold casting"),
    # Stamping / forming
    (re.compile(r'\bmetal\s+stamping\b', re.IGNORECASE), "metal stamping"),
    (re.compile(r'\bprogressive\s+die\b', re.IGNORECASE), "progressive die stamping"),
    (re.compile(r'\bdeep\s+draw(ing)?\b', re.IGNORECASE), "deep drawing"),
    (re.compile(r'\broll\s+forming\b', re.IGNORECASE), "roll forming"),
    (re.compile(r'\bhydroforming\b', re.IGNORECASE), "hydroforming"),
    # Welding / joining
    (re.compile(r'\bTIG\s+weld(ing)?\b', re.IGNORECASE), "TIG welding"),
    (re.compile(r'\bMIG\s+weld(ing)?\b', re.IGNORECASE), "MIG welding"),
    (re.compile(r'\brobotic\s+weld(ing)?\b', re.IGNORECASE), "robotic welding"),
    (re.compile(r'\bspot\s+weld(ing)?\b', re.IGNORECASE), "spot welding"),
    (re.compile(r'\bbrazin(g)?\b', re.IGNORECASE), "brazing"),
    (re.compile(r'\bfriction\s+stir\b', re.IGNORECASE), "friction stir welding"),
    # Fabrication / cutting
    (re.compile(r'\bsheet\s+metal\s+fabrication\b', re.IGNORECASE), "sheet metal fabrication"),
    (re.compile(r'\blaser\s+cutting\b', re.IGNORECASE), "laser cutting"),
    (re.compile(r'\bwaterjet\s+cutting\b', re.IGNORECASE), "waterjet cutting"),
    (re.compile(r'\bplasma\s+cutting\b', re.IGNORECASE), "plasma cutting"),
    (re.compile(r'\bpunch\s+press\b', re.IGNORECASE), "punch press"),
    # Additive / 3D printing
    (re.compile(r'\b3D\s+print(ing)?\b', re.IGNORECASE), "3D printing"),
    (re.compile(r'\badditive\s+manufacturing\b', re.IGNORECASE), "additive manufacturing"),
    (re.compile(r'\bSLS\b'), "SLS printing"),
    (re.compile(r'\bSLA\b'), "SLA printing"),
    (re.compile(r'\bFDM\b'), "FDM printing"),
    # Finishing / surface treatment
    (re.compile(r'\bheat\s+treat(ing|ment)?\b', re.IGNORECASE), "heat treating"),
    (re.compile(r'\bpowder\s+coat(ing)?\b', re.IGNORECASE), "powder coating"),
    (re.compile(r'\banodiz(ing|ation)?\b', re.IGNORECASE), "anodizing"),
    (re.compile(r'\belectroplat(ing|ed)?\b', re.IGNORECASE), "electroplating"),
    (re.compile(r'\bchrome\s+plat(ing|ed)?\b', re.IGNORECASE), "chrome plating"),
    (re.compile(r'\bnickel\s+plat(ing|ed)?\b', re.IGNORECASE), "nickel plating"),
    (re.compile(r'\bshot\s+blast(ing)?\b', re.IGNORECASE), "shot blasting"),
    (re.compile(r'\bsandblast(ing)?\b', re.IGNORECASE), "sandblasting"),
    (re.compile(r'\bpassivation\b', re.IGNORECASE), "passivation"),
    (re.compile(r'\bblack\s+oxide\b', re.IGNORECASE), "black oxide"),
    # Assembly
    (re.compile(r'\bsub[-\s]?assembly\b', re.IGNORECASE), "sub-assembly"),
    (re.compile(r'\bkitting\b', re.IGNORECASE), "kitting"),
    (re.compile(r'\bfinal\s+assembly\b', re.IGNORECASE), "final assembly"),
    # EDM
    (re.compile(r'\bwire\s+EDM\b', re.IGNORECASE), "wire EDM"),
    (re.compile(r'\bsinker\s+EDM\b', re.IGNORECASE), "sinker EDM"),
    (re.compile(r'\bEDM\b'), "EDM"),
    # Forging / extrusion
    (re.compile(r'\bforging\b', re.IGNORECASE), "forging"),
    (re.compile(r'\bextrusion\b', re.IGNORECASE), "extrusion"),
    (re.compile(r'\bcold\s+heading\b', re.IGNORECASE), "cold heading"),
    # Inspection / metrology
    (re.compile(r'\bCMM\b'), "CMM inspection"),
    (re.compile(r'\bfirst\s+article\s+inspection\b', re.IGNORECASE), "first article inspection"),
    (re.compile(r'\bNDT\b'), "NDT/non-destructive testing"),
    (re.compile(r'\bnon[-\s]?destructive\s+test(ing)?\b', re.IGNORECASE), "NDT/non-destructive testing"),
]

# Regex helpers
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
FOUNDED_RE = re.compile(r'\b(?:founded|established|since|incorporated)\s+(?:in\s+)?(\d{4})\b', re.IGNORECASE)

# Team/about page link patterns for discovery
TEAM_LINK_PATTERNS = re.compile(
    r'/(about|about-us|team|our-team|the-team|leadership|management|'
    r'management-team|our-leadership|executives|staff|people|company|'
    r'who-we-are|our-company)(?:[/-]|$)',
    re.IGNORECASE,
)

# Name + title extraction patterns
# Pattern 1: Schema.org Person JSON-LD handled separately
# Pattern 2: Adjacent heading/paragraph
NAME_TITLE_ADJACENT_RE = re.compile(
    r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s*)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'  # Name
    r'\s*[,\-\|]\s*'
    r'([A-Z][^\n,|<]{5,70})',  # Title
    re.MULTILINE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ua(idx: int) -> str:
    return USER_AGENTS[idx % len(USER_AGENTS)]


def _fetch(client: httpx.Client, url: str, ua_idx: int = 0) -> str | None:
    """GET url, return HTML text or None on failure."""
    headers = dict(STEALTH_HEADERS)
    headers["User-Agent"] = _ua(ua_idx)
    try:
        resp = client.get(url, headers=headers)
        if resp.status_code in (403, 429, 503):
            return None
        if resp.status_code >= 400:
            return None
        ct = resp.headers.get("content-type", "")
        if "html" not in ct and "text" not in ct:
            return None
        return resp.text
    except Exception:
        return None


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "html.parser")


def _text(soup: BeautifulSoup) -> str:
    """Return visible text from a soup object."""
    for tag in soup(["script", "style", "noscript", "svg", "head"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


def extract_meta(soup: BeautifulSoup) -> dict:
    """Extract meta description, OG description, page title, and tagline hints."""
    result = {}

    title_tag = soup.find("title")
    if title_tag:
        result["page_title"] = title_tag.get_text(strip=True)[:200]

    meta_desc = soup.find("meta", attrs={"name": re.compile(r"^description$", re.IGNORECASE)})
    if meta_desc:
        result["meta_description"] = meta_desc.get("content", "")[:500]

    og_desc = soup.find("meta", property="og:description")
    if og_desc and not result.get("meta_description"):
        result["meta_description"] = og_desc.get("content", "")[:500]

    # Hero tagline: first H1 or H2 that isn't the company name
    for tag in soup.find_all(["h1", "h2"])[:5]:
        text = tag.get_text(strip=True)
        if text and 10 < len(text) < 150:
            result.setdefault("tagline", text)
            break

    return result


def extract_schema_org(soup: BeautifulSoup) -> dict:
    """Extract Organization and Person data from schema.org JSON-LD."""
    result = {"executives": [], "founded_year": None}

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
        except (json.JSONDecodeError, TypeError):
            continue
        if not isinstance(data, dict):
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        _parse_schema_item(item, result)
            continue
        _parse_schema_item(data, result)

    return result


def _parse_schema_item(data: dict, result: dict) -> None:
    rtype = data.get("@type", "")
    if not isinstance(rtype, str):
        rtype = ""

    # Organization: founding year, employees
    if "Organization" in rtype or "Corporation" in rtype or "LocalBusiness" in rtype:
        if data.get("foundingDate"):
            try:
                year = int(str(data["foundingDate"])[:4])
                if 1800 < year < 2030:
                    result["founded_year"] = year
            except (ValueError, TypeError):
                pass

    # Person
    if rtype == "Person":
        name = data.get("name", "").strip()
        job = data.get("jobTitle", "").strip()
        email = data.get("email", "").strip().lstrip("mailto:")
        if name and job:
            result["executives"].append({"name": name, "title": job, "email": email, "source": "schema.org"})

    # Recurse into @graph
    for item in data.get("@graph", []):
        if isinstance(item, dict):
            _parse_schema_item(item, result)

    # Recurse into employee/founder arrays
    for key in ("employee", "founder", "member"):
        entries = data.get(key, [])
        if isinstance(entries, dict):
            entries = [entries]
        for entry in entries:
            if isinstance(entry, dict):
                _parse_schema_item(entry, result)


def find_team_links(soup: BeautifulSoup, base_url: str) -> list[str]:
    """Scan homepage links for team/about/leadership URLs."""
    candidates = []
    seen = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith(("mailto:", "tel:", "javascript:", "#")):
            continue
        full = urljoin(base_url, href)
        # Keep only same-domain links
        base_host = urlparse(base_url).netloc.lower().lstrip("www.")
        link_host = urlparse(full).netloc.lower().lstrip("www.")
        if base_host not in link_host and link_host not in base_host:
            continue
        path = urlparse(full).path
        if TEAM_LINK_PATTERNS.search(path):
            if full not in seen:
                seen.add(full)
                candidates.append(full)
    return candidates[:4]  # top candidates only


def extract_executives_from_dom(soup: BeautifulSoup, domain: str) -> list[dict]:
    """
    Extract executive contacts from typical team page HTML structures:
    - div.team-member / div.person / article.team with child h2/h3 (name) + span/p (title)
    - Definition list patterns
    - Plain text Name, Title patterns
    """
    execs = []
    seen_names = set()
    bare_domain = domain.replace("www.", "").split("/")[0]

    # --- Pattern 1: container-based team blocks ---
    TEAM_CONTAINERS = [
        # (container selector attrs, name tag, title tag)
        {"class": re.compile(r'team[_\-\s]?member', re.IGNORECASE)},
        {"class": re.compile(r'team[_\-\s]?card', re.IGNORECASE)},
        {"class": re.compile(r'person[_\-\s]?(card|block|item)?', re.IGNORECASE)},
        {"class": re.compile(r'leadership[_\-\s]?(card|block|item)?', re.IGNORECASE)},
        {"class": re.compile(r'staff[_\-\s]?(card|block|item)?', re.IGNORECASE)},
        {"class": re.compile(r'executive[_\-\s]?(card|block|item)?', re.IGNORECASE)},
        {"class": re.compile(r'employee[_\-\s]?(card|block|item)?', re.IGNORECASE)},
        {"class": re.compile(r'bio[_\-\s]?(card|block|item)?', re.IGNORECASE)},
        {"class": re.compile(r'profile[_\-\s]?(card|block|item)?', re.IGNORECASE)},
        {"itemtype": re.compile(r'Person', re.IGNORECASE)},
    ]

    for attrs in TEAM_CONTAINERS:
        for block in soup.find_all(True, attrs=attrs):
            # Name: prefer h2/h3/h4 inside block
            name_tag = block.find(["h2", "h3", "h4", "strong"])
            if not name_tag:
                continue
            name = name_tag.get_text(strip=True)
            if not _looks_like_person_name(name):
                continue
            if name in seen_names:
                continue

            # Title: next sibling paragraph/span/div after name tag
            title = ""
            for sibling in name_tag.find_next_siblings(["p", "span", "div", "em", "small"])[:3]:
                candidate = sibling.get_text(strip=True)
                if candidate and 3 < len(candidate) < 120 and _looks_like_title(candidate):
                    title = candidate
                    break

            if not title:
                # Try looking for a tag with "title" or "role" class
                role_tag = block.find(True, attrs={"class": re.compile(r'title|role|position|job', re.IGNORECASE)})
                if role_tag and role_tag != name_tag:
                    title = role_tag.get_text(strip=True)

            # Email: look inside block
            email = ""
            email_tag = block.find("a", href=re.compile(r'^mailto:', re.IGNORECASE))
            if email_tag:
                email = email_tag["href"].replace("mailto:", "").strip()
            else:
                raw = block.get_text()
                for m in EMAIL_RE.finditer(raw):
                    if bare_domain in m.group().lower():
                        email = m.group()
                        break

            if name and (title or email):
                seen_names.add(name)
                execs.append({"name": name, "title": title, "email": email, "source": "dom"})

    # --- Pattern 2: definition list <dt>Name</dt><dd>Title</dd> ---
    for dl in soup.find_all("dl"):
        dts = dl.find_all("dt")
        dds = dl.find_all("dd")
        for dt, dd in zip(dts, dds):
            name = dt.get_text(strip=True)
            title = dd.get_text(strip=True)
            if _looks_like_person_name(name) and _looks_like_title(title) and name not in seen_names:
                seen_names.add(name)
                execs.append({"name": name, "title": title, "email": "", "source": "dl"})

    # --- Pattern 3: heading followed immediately by plain-text title ---
    for heading in soup.find_all(["h2", "h3", "h4"]):
        name = heading.get_text(strip=True)
        if not _looks_like_person_name(name) or name in seen_names:
            continue
        # Check the very next element
        nxt = heading.find_next_sibling()
        if nxt:
            candidate = nxt.get_text(strip=True)
            if _looks_like_title(candidate) and len(candidate) < 100:
                seen_names.add(name)
                execs.append({"name": name, "title": candidate, "email": "", "source": "heading"})

    # --- Pattern 4: Name — Title / Name, Title text patterns in paragraphs ---
    for p in soup.find_all(["p", "li"])[:200]:
        text = p.get_text(strip=True)
        if len(text) < 8 or len(text) > 300:
            continue
        m = NAME_TITLE_ADJACENT_RE.search(text)
        if m:
            name = m.group(1).strip()
            title = m.group(2).strip()
            if _looks_like_person_name(name) and _looks_like_title(title) and name not in seen_names:
                seen_names.add(name)
                execs.append({"name": name, "title": title, "email": "", "source": "text"})

    return execs


def _looks_like_person_name(text: str) -> bool:
    """Heuristic: 2-4 words, each starting with a capital."""
    if not text:
        return False
    words = text.split()
    if not (2 <= len(words) <= 5):
        return False
    if not all(w[0].isupper() for w in words if len(w) > 1):
        return False
    # Reject known non-names
    BAD = {"Read", "More", "View", "Contact", "About", "Meet", "Our", "Team",
           "The", "Click", "Here", "Learn", "Get", "Visit", "See", "Home"}
    if any(w in BAD for w in words):
        return False
    return True


def _looks_like_title(text: str) -> bool:
    """Heuristic: plausible job title."""
    if not text or len(text) < 3 or len(text) > 120:
        return False
    # Must contain at least one recognized title word
    lower = text.lower()
    TITLE_WORDS = [
        "ceo", "president", "owner", "founder", "director", "manager",
        "officer", "vp", "vice", "controller", "cfo", "coo", "cto", "cio",
        "engineer", "operations", "sales", "marketing", "finance", "supply",
        "purchasing", "plant", "general", "executive", "partner",
    ]
    return any(w in lower for w in TITLE_WORDS)


def filter_decision_makers(execs: list[dict]) -> list[dict]:
    """Return only executives matching target decision-maker titles."""
    results = []
    for exec_rec in execs:
        title_lower = exec_rec.get("title", "").lower()
        if any(t in title_lower for t in TARGET_TITLES):
            results.append(exec_rec)
    return results


def detect_certifications(text: str) -> list[str]:
    certs = []
    seen = set()
    for pattern, label in CERT_PATTERNS:
        if pattern.search(text) and label not in seen:
            certs.append(label)
            seen.add(label)
    return certs


def detect_capabilities(text: str) -> list[str]:
    caps = []
    seen = set()
    for pattern, label in CAPABILITY_PATTERNS:
        if pattern.search(text) and label not in seen:
            caps.append(label)
            seen.add(label)
    return caps


def detect_founded_year(text: str) -> int | None:
    m = FOUNDED_RE.search(text)
    if m:
        year = int(m.group(1))
        if 1800 < year < 2030:
            return year
    return None


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_companies(limit: int | None, assoc: str | None) -> list[dict]:
    """Load companies with websites from the master CSV."""
    records = []
    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            website = (row.get("website") or "").strip()
            domain = (row.get("domain") or "").strip()

            if not website and not domain:
                continue

            if not website:
                website = f"https://{domain}"
            if not website.startswith("http"):
                website = f"https://{website}"

            if assoc:
                assoc_col = (row.get("associations") or "").upper()
                if assoc.upper() not in assoc_col:
                    continue

            records.append({
                "company_name": row.get("company_name", "").strip(),
                "website": website,
                "domain": domain,
                "associations": row.get("associations", ""),
                "state": row.get("state", ""),
            })

    if limit:
        records = records[:limit]
    return records


def load_already_crawled() -> set[str]:
    """Return domains already written to OUTPUT_PATH."""
    seen = set()
    if not OUTPUT_PATH.exists():
        return seen
    with open(OUTPUT_PATH, encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
                d = rec.get("domain", "")
                if d:
                    seen.add(d)
            except json.JSONDecodeError:
                pass
    return seen


# ---------------------------------------------------------------------------
# Core crawl logic per company
# ---------------------------------------------------------------------------

def crawl_company(client: httpx.Client, company: dict, ua_idx: int) -> dict:
    """
    Crawl up to MAX_PAGES_PER_COMPANY pages for a company and return enrichment record.
    """
    base_url = company["website"]
    domain = company["domain"] or urlparse(base_url).netloc.lstrip("www.")

    result = {
        "company_name": company["company_name"],
        "domain": domain,
        "website": base_url,
        "associations": company.get("associations", ""),
        "meta_description": "",
        "tagline": "",
        "executives": [],
        "certifications": [],
        "manufacturing_capabilities": [],
        "founded_year": None,
        "pages_crawled": 0,
        "pages_attempted": 0,
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }

    all_text_chunks = []  # combined text across pages for cert/cap scan
    pages_budget = MAX_PAGES_PER_COMPANY
    pages_crawled = 0

    # ------------------------------------------------------------------
    # Page 1: Homepage
    # ------------------------------------------------------------------
    result["pages_attempted"] += 1
    html = _fetch(client, base_url, ua_idx)
    if not html:
        return result

    soup = _soup(html)
    meta = extract_meta(soup)
    result["meta_description"] = meta.get("meta_description", "")
    result["tagline"] = meta.get("tagline", "")

    schema_data = extract_schema_org(soup)
    if schema_data.get("founded_year"):
        result["founded_year"] = schema_data["founded_year"]

    for exec_rec in schema_data.get("executives", []):
        if exec_rec.get("name") not in {e["name"] for e in result["executives"]}:
            result["executives"].append(exec_rec)

    homepage_text = _text(soup)
    all_text_chunks.append(homepage_text)

    if not result["founded_year"]:
        result["founded_year"] = detect_founded_year(homepage_text)

    # Discover team/about links from homepage
    team_links = find_team_links(soup, base_url)
    pages_crawled += 1
    pages_budget -= 1

    # ------------------------------------------------------------------
    # Page 2: Best team/about/leadership page
    # ------------------------------------------------------------------
    team_page_fetched = False
    for team_url in team_links[:2]:
        if pages_budget <= 0:
            break
        time.sleep(DELAY_BETWEEN_REQUESTS)
        result["pages_attempted"] += 1
        team_html = _fetch(client, team_url, ua_idx)
        if not team_html:
            continue

        team_soup = _soup(team_html)
        team_text = _text(team_soup)
        all_text_chunks.append(team_text)

        # Extract executives from DOM first (more precise)
        dom_execs = extract_executives_from_dom(team_soup, domain)
        existing_names = {e["name"] for e in result["executives"]}
        for exec_rec in dom_execs:
            if exec_rec["name"] not in existing_names:
                result["executives"].append(exec_rec)
                existing_names.add(exec_rec["name"])

        # Schema.org on team page too
        schema_team = extract_schema_org(team_soup)
        for exec_rec in schema_team.get("executives", []):
            if exec_rec["name"] not in existing_names:
                result["executives"].append(exec_rec)
                existing_names.add(exec_rec["name"])

        if not result["founded_year"]:
            result["founded_year"] = detect_founded_year(team_text)

        pages_crawled += 1
        pages_budget -= 1
        team_page_fetched = True
        break  # one team page is enough

    # ------------------------------------------------------------------
    # Page 3: /contact page
    # ------------------------------------------------------------------
    if pages_budget > 0:
        contact_url = urljoin(base_url, "/contact")
        time.sleep(DELAY_BETWEEN_REQUESTS)
        result["pages_attempted"] += 1
        contact_html = _fetch(client, contact_url, ua_idx)
        if contact_html and len(contact_html) > 500:
            contact_soup = _soup(contact_html)
            contact_text = _text(contact_soup)
            all_text_chunks.append(contact_text)

            # Pick up any execs mentioned on contact page too
            dom_execs = extract_executives_from_dom(contact_soup, domain)
            existing_names = {e["name"] for e in result["executives"]}
            for exec_rec in dom_execs:
                if exec_rec["name"] not in existing_names:
                    result["executives"].append(exec_rec)
                    existing_names.add(exec_rec["name"])

            pages_crawled += 1
            pages_budget -= 1

    # ------------------------------------------------------------------
    # Page 4: /capabilities, /services, or /manufacturing if budget allows
    # ------------------------------------------------------------------
    CAPABILITY_PATHS = ["/capabilities", "/services", "/manufacturing",
                        "/what-we-do", "/our-capabilities", "/processes"]
    if pages_budget > 0 and not team_page_fetched:
        # If no team page was found, try a cap page for cert/cap scanning
        for cap_path in CAPABILITY_PATHS:
            if pages_budget <= 0:
                break
            cap_url = urljoin(base_url, cap_path)
            time.sleep(DELAY_BETWEEN_REQUESTS)
            result["pages_attempted"] += 1
            cap_html = _fetch(client, cap_url, ua_idx)
            if cap_html and len(cap_html) > 500:
                cap_soup = _soup(cap_html)
                all_text_chunks.append(_text(cap_soup))
                pages_crawled += 1
                pages_budget -= 1
                break
    elif pages_budget > 0:
        # Use remaining budget on a capabilities page to improve cert/cap detection
        for cap_path in CAPABILITY_PATHS:
            if pages_budget <= 0:
                break
            cap_url = urljoin(base_url, cap_path)
            time.sleep(DELAY_BETWEEN_REQUESTS)
            result["pages_attempted"] += 1
            cap_html = _fetch(client, cap_url, ua_idx)
            if cap_html and len(cap_html) > 500:
                all_text_chunks.append(_text(_soup(cap_html)))
                pages_crawled += 1
                pages_budget -= 1
                break

    # ------------------------------------------------------------------
    # Post-process: scan all text for certs/capabilities
    # ------------------------------------------------------------------
    combined_text = " ".join(all_text_chunks)
    result["certifications"] = detect_certifications(combined_text)
    result["manufacturing_capabilities"] = detect_capabilities(combined_text)
    result["pages_crawled"] = pages_crawled

    # Filter executives to decision-makers only, but keep all if none match
    dm_execs = filter_decision_makers(result["executives"])
    result["executives"] = dm_execs if dm_execs else result["executives"][:10]

    # Deduplicate executives by name
    seen_names: set[str] = set()
    deduped = []
    for exec_rec in result["executives"]:
        n = exec_rec.get("name", "")
        if n and n not in seen_names:
            seen_names.add(n)
            deduped.append(exec_rec)
    result["executives"] = deduped

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Deep website enrichment: executives, certs, capabilities")
    parser.add_argument("--limit", type=int, default=200, help="Max companies to crawl (default 200)")
    parser.add_argument("--assoc", type=str, help="Filter by association code (e.g. AGMA)")
    parser.add_argument("--resume", action="store_true", help="Skip domains already in output file")
    args = parser.parse_args()

    print("=" * 65)
    print("NAM Intelligence Pipeline - Deep Website Enrichment")
    print("=" * 65)

    companies = load_companies(limit=args.limit, assoc=args.assoc)
    print(f"\nLoaded {len(companies)} companies from CSV")

    already_crawled: set[str] = set()
    if args.resume:
        already_crawled = load_already_crawled()
        before = len(companies)
        companies = [c for c in companies if c["domain"] not in already_crawled]
        print(f"  Resume mode: skipping {before - len(companies)} already-crawled domains")

    print(f"  To crawl: {len(companies)}")
    if args.assoc:
        print(f"  Association filter: {args.assoc}")
    print(f"  Output: {OUTPUT_PATH}")
    print()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    # Open output file in append mode to support resume
    mode = "a" if args.resume else "w"

    stats = {
        "companies": 0,
        "with_executives": 0,
        "total_executives": 0,
        "with_certifications": 0,
        "with_capabilities": 0,
        "errors": 0,
    }

    client = httpx.Client(
        follow_redirects=True,
        timeout=REQUEST_TIMEOUT,
    )

    try:
        with open(OUTPUT_PATH, mode, encoding="utf-8") as out_f:
            for i, company in enumerate(companies):
                try:
                    record = crawl_company(client, company, ua_idx=i)
                except Exception as exc:
                    stats["errors"] += 1
                    record = {
                        "company_name": company["company_name"],
                        "domain": company["domain"],
                        "error": str(exc),
                        "crawled_at": datetime.now(timezone.utc).isoformat(),
                    }

                out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                out_f.flush()

                # Stats
                stats["companies"] += 1
                if record.get("executives"):
                    stats["with_executives"] += 1
                    stats["total_executives"] += len(record["executives"])
                if record.get("certifications"):
                    stats["with_certifications"] += 1
                if record.get("manufacturing_capabilities"):
                    stats["with_capabilities"] += 1

                # Progress print every 25 companies
                if (i + 1) % 25 == 0:
                    pct = (i + 1) / len(companies) * 100
                    print(
                        f"  [{i+1:4d}/{len(companies)}] {pct:5.1f}%  "
                        f"Execs: {stats['with_executives']}  "
                        f"Certs: {stats['with_certifications']}  "
                        f"Caps: {stats['with_capabilities']}  "
                        f"Errors: {stats['errors']}"
                    )

                # Rate limit between companies
                if i < len(companies) - 1:
                    time.sleep(DELAY_BETWEEN_COMPANIES)

    finally:
        client.close()

    print()
    print("=" * 65)
    print("Results")
    print("=" * 65)
    print(f"  Companies crawled:         {stats['companies']}")
    print(f"  With executives found:     {stats['with_executives']}")
    print(f"  Total executives:          {stats['total_executives']}")
    print(f"  With certifications:       {stats['with_certifications']}")
    print(f"  With capabilities:         {stats['with_capabilities']}")
    print(f"  Errors / skipped:          {stats['errors']}")
    print(f"  Output:                    {OUTPUT_PATH}")

    if stats["companies"] > 0:
        print(f"\n  Exec hit rate:   {stats['with_executives'] / stats['companies'] * 100:.1f}%")
        print(f"  Cert hit rate:   {stats['with_certifications'] / stats['companies'] * 100:.1f}%")
        print(f"  Cap hit rate:    {stats['with_capabilities'] / stats['companies'] * 100:.1f}%")

    print("\nDone!")


if __name__ == "__main__":
    main()
