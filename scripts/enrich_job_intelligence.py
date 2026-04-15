#!/usr/bin/env python3
"""
Job Intelligence Enrichment
NAM Intelligence Pipeline

Scrapes public Indeed job listings to detect competitor ERP systems,
hiring signals, and tech stack from job descriptions.

Job descriptions reveal the ACTUAL internal tech stack — when a company
posts "Experience with Epicor ERP required", that's a confirmed competitor
detection that DNS/website scanning can never find.

Indeed requires JavaScript execution (Cloudflare challenge), so this script
uses Playwright in headed mode with stealth init script (same pattern used
for NEMA/AGMA extractions).

Usage:
    python scripts/enrich_job_intelligence.py            # all companies
    python scripts/enrich_job_intelligence.py --limit 100  # first 100 (alpha order)
    python scripts/enrich_job_intelligence.py --resume    # skip already-scraped

Output: data/processed/job_intelligence.jsonl
"""

import argparse
import csv
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote_plus

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CSV_PATH = PROJECT_ROOT / "data" / "exports" / "companies_all.csv"
OUTPUT_PATH = PROJECT_ROOT / "data" / "processed" / "job_intelligence.jsonl"

# ── Rate limiting ──────────────────────────────────────────────────────────────
RATE_LIMIT_DELAY = 5       # seconds between Indeed searches (be respectful)
BACKOFF_DELAY = 30         # seconds after a block detection
CAPTCHA_BACKOFF = 60       # seconds on captcha detection
PAGE_LOAD_TIMEOUT = 25_000  # ms
SELECTOR_TIMEOUT = 12_000   # ms

# ── Stealth init script (same as BaseAgent._STEALTH_SCRIPT) ───────────────────
_STEALTH_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {
        get: () => [1, 2, 3, 4, 5],
    });
    window.chrome = { runtime: {} };
"""

# ── ERP competitor keyword map ─────────────────────────────────────────────────
ERP_KEYWORDS: dict[str, list[str]] = {
    "epicor": ["epicor", "epicor kinetic", "prophet 21", "p21", "epicor erp"],
    "sap": ["sap erp", "sap business one", "sap b1", "s/4hana", "sap s4", "sapui5"],
    "oracle": ["oracle erp", "jd edwards", "jde", "oracle cloud erp", "oracle e-business"],
    "netsuite": ["netsuite", "oracle netsuite"],
    "infor": ["infor cloudsuite", "syteline", "infor m3", "infor ln", "infor visual"],
    "microsoft dynamics": [
        "dynamics 365", "dynamics nav", "dynamics ax", "d365 finance",
        "business central", "ms dynamics",
    ],
    "syspro": ["syspro"],
    "acumatica": ["acumatica"],
    "plex": ["plex manufacturing", "plex erp", "plex systems"],
    "sage": ["sage 100", "sage 300", "sage x3", "sage intacct", "sage 50", "sage erp"],
    "qad": ["qad erp", "qad cloud"],
    "iqms": ["iqms", "delmiaworks"],
    "global shop solutions": ["global shop solutions", "global shop erp"],
    "jobboss": ["jobboss", "job boss"],
    "aptean": ["aptean"],
    "made2manage": ["made2manage", "m2m erp"],
    "macola": ["macola"],
    "visual manufacturing": ["visual manufacturing", "infor visual"],
    "e2 shop system": ["e2 shop", "e2 manufacturing"],
    "shoptech": ["shoptech", "e2 shop system"],
    "rootstock": ["rootstock erp", "rootstock cloud"],
    "erpnext": ["erpnext"],
    "odoo": ["odoo erp"],
}

# ── Additional tech stack keywords ────────────────────────────────────────────
TECH_KEYWORDS: dict[str, list[str]] = {
    "crm": ["salesforce", "hubspot crm", "dynamics crm", "zoho crm", "pipedrive", "sugar crm"],
    "cad": [
        "solidworks", "autocad", "catia", "creo parametric", "ptc creo",
        "autodesk inventor", "nx cad", "fusion 360", "mastercam",
    ],
    "mes": [
        "mes system", "manufacturing execution system", "plex mes",
        "aegis", "42q", "opcenter",
    ],
    "plm": [
        "plm", "product lifecycle management", "ptc windchill", "windchill",
        "siemens teamcenter", "teamcenter", "arena plm", "agile plm",
    ],
    "quality": [
        "minitab", "quality management system", "qms", "etq",
        "mastercontrol", "intelex",
    ],
    "warehouse": ["wms", "warehouse management system", "fishbowl", "3pl"],
    "bi": [
        "power bi", "tableau", "qlik", "qlikview", "qliksense", "looker",
        "business intelligence", "crystal reports", "ssrs",
    ],
    "scm": ["sap ariba", "ariba", "coupa", "jaggaer", "supply chain management"],
}

GROWTH_SIGNALS = re.compile(
    r"\b(urgently\s+hiring|hiring\s+now|immediate\s+opening|"
    r"multiple\s+openings|fast[-\s]growing|rapid\s+growth)\b",
    re.IGNORECASE,
)


# ── Data loading ───────────────────────────────────────────────────────────────

def load_companies(limit: int | None = None) -> list[dict]:
    """Load companies from companies_all.csv, sorted alphabetically."""
    if not CSV_PATH.exists():
        print(f"Error: {CSV_PATH} not found")
        sys.exit(1)

    companies: list[dict] = []
    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("company_name") or "").strip()
            if name:
                companies.append({
                    "company_name": name,
                    "domain": (row.get("domain") or "").strip(),
                    "city": (row.get("city") or "").strip(),
                    "state": (row.get("state") or "").strip(),
                })

    companies.sort(key=lambda r: r["company_name"].lower())
    if limit:
        companies = companies[:limit]
    return companies


def load_existing_results() -> dict[str, dict]:
    """Load already-scraped results keyed by company_name."""
    existing: dict[str, dict] = {}
    if OUTPUT_PATH.exists():
        with open(OUTPUT_PATH, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                    existing[rec["company_name"]] = rec
                except (json.JSONDecodeError, KeyError):
                    continue
    return existing


# ── Indeed scraping ────────────────────────────────────────────────────────────

def build_indeed_url(company_name: str) -> str:
    """Build an Indeed search URL for a company name."""
    q = quote_plus(f'"{company_name}"')
    return f"https://www.indeed.com/jobs?q={q}&l=&sort=date"


def _is_blocked(html: str) -> bool:
    """Detect bot/captcha blocks."""
    lower = html.lower()
    return (
        "captcha" in lower
        or "unusual traffic" in lower
        or "verify you are human" in lower
        or 'id="challenge-form"' in lower
        or "authenticating..." in lower
        or "redirecting to login" in lower
    )


def extract_jobs_from_html(html: str) -> list[dict]:
    """Extract job cards from Indeed HTML via the embedded mosaic JSON blob.

    Indeed embeds job data in:
        window.mosaic.providerData["mosaic-provider-jobcards"] = {...};

    This is far more reliable than HTML scraping because it's structured JSON
    that includes: title, company, location, snippet, urgentlyHiring, pubDate, salary.
    """
    m = re.search(
        r'window\.mosaic\.providerData\["mosaic-provider-jobcards"\]\s*=\s*(\{.*?\});',
        html,
        re.DOTALL,
    )
    if not m:
        return []

    try:
        data = json.loads(m.group(1))
    except json.JSONDecodeError:
        return []

    raw_results = (
        data.get("metaData", {})
            .get("mosaicProviderJobCardsModel", {})
            .get("results", [])
    )

    jobs = []
    for item in raw_results:
        title = (item.get("title") or item.get("displayTitle") or "").strip()
        if not title:
            continue

        company = (item.get("company") or item.get("truncatedCompany") or "").strip()
        location = (item.get("formattedLocation") or "").strip()
        posted = (item.get("formattedRelativeTime") or "").strip()
        urgent = bool(item.get("urgentlyHiring", False))

        # snippet is HTML — strip tags
        snippet_raw = item.get("snippet") or ""
        snippet = re.sub(r"<[^>]+>", " ", snippet_raw)
        snippet = re.sub(r"\s+", " ", snippet).strip()[:600]

        # Salary hint
        salary_obj = item.get("extractedSalary") or item.get("salarySnippet")
        if isinstance(salary_obj, dict):
            salary = salary_obj.get("text") or salary_obj.get("salary") or ""
        else:
            salary = ""

        jobs.append({
            "title": title,
            "employer": company,
            "location": location,
            "snippet": snippet,
            "posted": posted,
            "salary": str(salary).strip(),
            "urgent": urgent,
        })

    return jobs


def _company_name_matches(target: str, employer: str, snippet: str) -> bool:
    """Loosely verify the job card is from (or closely related to) the target company.

    Indeed search results can surface jobs from other companies that merely
    mention the target name in the description. We accept if:
    - employer contains a significant word from the target name, OR
    - the snippet contains the target name
    We reject if the employer is clearly unrelated (non-empty and shares no tokens).
    """
    target_lower = target.lower().strip()
    employer_lower = employer.lower().strip()
    snippet_lower = snippet.lower()

    # Exact / substring match on employer name
    if target_lower in employer_lower or employer_lower in target_lower:
        return True

    # Significant words (>3 chars, not stopwords) in target that appear in employer
    stopwords = {"inc", "corp", "llc", "ltd", "co", "company", "the", "and",
                 "group", "solutions", "systems", "services", "industries",
                 "manufacturing", "international"}
    words = [
        w for w in re.split(r"[\s,./&\-]+", target_lower)
        if len(w) > 3 and w not in stopwords
    ]
    if words:
        if any(w in employer_lower for w in words[:2]):
            return True

    # Target name appears in the job snippet (consulting/partner scenario)
    if target_lower in snippet_lower:
        return True

    # If we couldn't parse an employer name, accept the card
    if not employer_lower:
        return True

    return False


def filter_jobs(jobs: list[dict], company_name: str) -> list[dict]:
    """Return only jobs that plausibly belong to the target company."""
    return [
        j for j in jobs
        if _company_name_matches(company_name, j["employer"], j["snippet"])
    ]


# ── Signal detection ───────────────────────────────────────────────────────────

def detect_erp(jobs: list[dict]) -> tuple[str | None, list[dict]]:
    """Scan job snippets and titles for ERP keyword mentions.

    Returns (primary_erp_vendor_or_None, list_of_mention_dicts).
    primary vendor = the most frequently mentioned one.
    """
    mentions: list[dict] = []
    vendor_counts: Counter = Counter()

    for job in jobs:
        text = (job["title"] + " " + job["snippet"]).lower()

        for vendor, keywords in ERP_KEYWORDS.items():
            for keyword in keywords:
                kw_lower = keyword.lower()
                pos = text.find(kw_lower)
                while pos != -1:
                    start = max(0, pos - 60)
                    end = min(len(text), pos + len(kw_lower) + 60)
                    context = "..." + text[start:end].strip() + "..."

                    mentions.append({
                        "vendor": vendor,
                        "keyword": keyword,
                        "context": context,
                        "job_title": job["title"],
                    })
                    vendor_counts[vendor] += 1

                    # Advance past this occurrence to avoid infinite loop
                    pos = text.find(kw_lower, pos + 1)

    # Dedupe mentions (same vendor+keyword+job_title)
    seen: set[tuple] = set()
    deduped: list[dict] = []
    for m in mentions:
        key = (m["vendor"], m["keyword"], m["job_title"])
        if key not in seen:
            seen.add(key)
            deduped.append(m)

    primary = vendor_counts.most_common(1)[0][0] if vendor_counts else None
    return primary, deduped[:15]  # cap to keep JSONL records lean


def detect_tech(jobs: list[dict]) -> dict[str, list[str]]:
    """Scan job text for additional tech stack signals."""
    combined = " ".join(
        (j["title"] + " " + j["snippet"]).lower()
        for j in jobs
    )
    detected: dict[str, list[str]] = {}
    for category, keywords in TECH_KEYWORDS.items():
        found = [kw for kw in keywords if kw.lower() in combined]
        if found:
            detected[category] = list(dict.fromkeys(found))  # preserve order, dedupe
    return detected


def assess_hiring_signal(jobs: list[dict]) -> str:
    """Classify hiring activity level."""
    count = len(jobs)
    urgent = sum(1 for j in jobs if j.get("urgent"))
    if count == 0:
        return "none"
    if count >= 10 or urgent >= 2:
        return "high"
    if count >= 4:
        return "active"
    return "low"


def extract_hiring_titles(jobs: list[dict]) -> list[str]:
    """Return unique job titles, capped at 20."""
    seen: set[str] = set()
    titles: list[str] = []
    for j in jobs:
        t = j["title"].strip()
        if t and t not in seen:
            seen.add(t)
            titles.append(t)
    return titles[:20]


# ── Browser session ────────────────────────────────────────────────────────────

class IndeedScraper:
    """Manages a single headed Playwright browser session for all Indeed lookups."""

    def __init__(self):
        from playwright.sync_api import sync_playwright
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=False)
        self._context = self._browser.new_context(
            viewport={"width": 1280, "height": 800},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
            locale="en-US",
            timezone_id="America/New_York",
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;"
                    "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
                ),
            },
        )
        self._context.add_init_script(_STEALTH_SCRIPT)
        self._page = self._context.new_page()

    def close(self):
        try:
            self._browser.close()
        except Exception:
            pass
        try:
            self._pw.stop()
        except Exception:
            pass

    def fetch(self, url: str) -> tuple[str | None, str]:
        """Fetch a URL and return (html_or_None, status_tag).

        status_tag is one of: 'ok', 'blocked', 'captcha', 'error'.
        """
        try:
            self._page.goto(url, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")
        except Exception as e:
            err_msg = str(e).lower()
            if "timeout" in err_msg:
                return None, "error"
            return None, "error"

        # Wait briefly for JS to inject the mosaic data
        try:
            self._page.wait_for_function(
                "() => typeof window.mosaic !== 'undefined'",
                timeout=SELECTOR_TIMEOUT,
            )
        except Exception:
            pass  # page may not have mosaic at all (0 results is fine)

        html = self._page.content()

        if _is_blocked(html):
            return None, "blocked"

        return html, "ok"

    def scrape_company(self, company: dict) -> dict:
        """Scrape Indeed for one company and return a result record."""
        name = company["company_name"]
        url = build_indeed_url(name)
        now = datetime.now(timezone.utc).isoformat()

        base_result = {
            "company_name": name,
            "domain": company["domain"],
            "job_count": 0,
            "erp_detected": None,
            "erp_mentions": [],
            "tech_detected": {},
            "hiring_titles": [],
            "hiring_signal": "none",
            "scraped_at": now,
        }

        html, status = self.fetch(url)

        if status == "blocked":
            print(f"      [blocked/captcha] backing off {CAPTCHA_BACKOFF}s")
            time.sleep(CAPTCHA_BACKOFF)
            # Retry once after backoff
            html, status = self.fetch(url)
            if html is None:
                base_result["hiring_signal"] = "error"
                return base_result

        if html is None:
            base_result["hiring_signal"] = "error"
            return base_result

        all_jobs = extract_jobs_from_html(html)
        jobs = filter_jobs(all_jobs, name)

        erp, mentions = detect_erp(jobs)
        tech = detect_tech(jobs)
        signal = assess_hiring_signal(jobs)
        titles = extract_hiring_titles(jobs)

        return {
            **base_result,
            "job_count": len(jobs),
            "erp_detected": erp,
            "erp_mentions": mentions,
            "tech_detected": tech,
            "hiring_titles": titles,
            "hiring_signal": signal,
        }


# ── Output helpers ─────────────────────────────────────────────────────────────

def print_result(result: dict) -> None:
    name = result["company_name"][:40].ljust(40)
    jobs = result["job_count"]
    erp = result["erp_detected"] or "-"
    signal = result["hiring_signal"]
    tech_cats = ", ".join(result.get("tech_detected", {}).keys()) or "-"
    erp_tag = f"  ERP={erp}" if result["erp_detected"] else ""
    print(
        f"  {name}  jobs={jobs:<3}  signal={signal:<6}  "
        f"tech=[{tech_cats[:28]}]{erp_tag}"
    )


def print_summary(results: list[dict]) -> None:
    total = len(results)
    if total == 0:
        return

    with_jobs = sum(1 for r in results if r["job_count"] > 0)
    erp_detected = [r for r in results if r["erp_detected"]]
    all_erps: Counter = Counter(r["erp_detected"] for r in erp_detected)
    signals: Counter = Counter(r["hiring_signal"] for r in results)

    all_tech: Counter = Counter()
    for r in results:
        for cat in r.get("tech_detected", {}):
            all_tech[cat] += 1

    errors = sum(1 for r in results if r["hiring_signal"] == "error")

    print()
    print("=" * 65)
    print("Job Intelligence — Summary")
    print("=" * 65)
    print(f"  Companies processed:    {total}")
    print(f"  With job listings:      {with_jobs} ({with_jobs/total*100:.1f}%)")
    print(f"  ERP confirmed:          {len(erp_detected)} ({len(erp_detected)/total*100:.1f}%)")
    print(f"  Errors:                 {errors}")

    print()
    print("  Hiring Signal Distribution:")
    for sig, cnt in signals.most_common():
        print(f"    {sig:<10}  {cnt}")

    if all_erps:
        print()
        print("  ERP Systems Detected:")
        for erp, cnt in all_erps.most_common():
            print(f"    {erp:<35}  {cnt}")

    if all_tech:
        print()
        print("  Tech Category Mentions:")
        for cat, cnt in all_tech.most_common():
            print(f"    {cat:<20}  {cnt}")

    print()
    print(f"  Output: {OUTPUT_PATH}")
    print("=" * 65)


# ── Entry point ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Indeed job listings for ERP and tech stack intelligence"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Max number of companies to process (default: all)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip companies already present in the output file",
    )
    args = parser.parse_args()

    print("=" * 65)
    print("NAM Intelligence Pipeline — Job Intelligence Enrichment")
    print("=" * 65)

    companies = load_companies(limit=args.limit)
    print(f"\nLoaded {len(companies)} companies (alphabetical order)")

    existing: dict[str, dict] = {}
    if args.resume:
        existing = load_existing_results()
        print(f"Resume mode: {len(existing)} already scraped")

    to_process = [c for c in companies if c["company_name"] not in existing]
    print(f"To process:  {len(to_process)} companies")
    print(f"Rate limit:  {RATE_LIMIT_DELAY}s between requests")
    estimated = len(to_process) * RATE_LIMIT_DELAY
    print(f"Est. time:   ~{estimated // 60}m {estimated % 60}s")
    print()

    if not to_process:
        print("Nothing to do.")
        return

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    out_mode = "a" if args.resume and OUTPUT_PATH.exists() else "w"
    results_this_run: list[dict] = []

    scraper = IndeedScraper()
    try:
        with open(OUTPUT_PATH, out_mode, encoding="utf-8") as out_f:
            for i, company in enumerate(to_process):
                result = scraper.scrape_company(company)
                results_this_run.append(result)

                # Write immediately — crash-safe incremental output
                out_f.write(json.dumps(result, ensure_ascii=False) + "\n")
                out_f.flush()

                print_result(result)

                if (i + 1) % 25 == 0:
                    erp_so_far = sum(1 for r in results_this_run if r["erp_detected"])
                    jobs_so_far = sum(1 for r in results_this_run if r["job_count"] > 0)
                    print(
                        f"\n  --- Progress: {i+1}/{len(to_process)} | "
                        f"with jobs: {jobs_so_far} | ERP detected: {erp_so_far} ---\n"
                    )

                if i < len(to_process) - 1:
                    time.sleep(RATE_LIMIT_DELAY)

    finally:
        scraper.close()

    print_summary(results_this_run)


if __name__ == "__main__":
    main()
