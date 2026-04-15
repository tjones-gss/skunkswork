#!/usr/bin/env python3
"""
Job Intelligence Enrichment
NAM Intelligence Pipeline

Scrapes public Indeed job listings to detect competitor ERP systems,
hiring signals, and tech stack from job descriptions.

Job descriptions reveal the ACTUAL internal tech stack — when a company
posts "Experience with Epicor ERP required", that's a confirmed competitor
detection that DNS/website scanning can never find.

Indeed uses Cloudflare Turnstile bot protection.  This script uses Playwright
in headed mode so the Cloudflare challenge can be solved (or manually completed
if needed).  Session cookies are reused across all company lookups, so you
only need to pass the CF challenge once at startup.

Usage:
    python scripts/enrich_job_intelligence.py            # all companies
    python scripts/enrich_job_intelligence.py --limit 100  # first 100 (alpha order)
    python scripts/enrich_job_intelligence.py --resume    # skip already-scraped
    python scripts/enrich_job_intelligence.py --limit 50 --wait-for-cf
      # Opens browser, pauses 45s so you can manually click past CF challenge

Output: data/processed/job_intelligence.jsonl

Notes on Indeed access:
    - Cloudflare Turnstile may block automated sessions for the /jobs endpoint
    - The browser is opened in headed mode so CF challenges can auto-solve
    - If CF blocks, wait a few hours and re-run with --resume
    - ERP/tech signals already in enriched_all.jsonl (from DNS/web scraping)
      are more reliable anyway; job intelligence is a supplemental signal
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
RATE_LIMIT_DELAY = 5          # seconds between Indeed searches
CAPTCHA_BACKOFF = 60          # seconds after a block, before retry
PAGE_LOAD_TIMEOUT = 30_000    # ms — longer timeout for CF challenge resolution
CF_WAIT_TIMEOUT = 20_000      # ms — wait for CF challenge to auto-solve
SELECTOR_TIMEOUT = 12_000     # ms — wait for mosaic JSON to be injected

# ── Stealth init script (same pattern as BaseAgent._STEALTH_SCRIPT) ───────────
_STEALTH_SCRIPT = """
    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
    Object.defineProperty(navigator, 'plugins', {
        get: () => [
            {name: 'Chrome PDF Plugin', filename: 'internal-pdf-viewer', description: 'Portable Document Format'},
            {name: 'Chrome PDF Viewer', filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai', description: 'Portable Document Format'},
            {name: 'Native Client', filename: 'internal-nacl-plugin', description: ''},
        ],
    });
    window.chrome = { runtime: {}, loadTimes: function() {}, csi: function() {} };
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    Object.defineProperty(navigator, 'platform', { get: () => 'Win32' });
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
        "product lifecycle management", "ptc windchill", "windchill",
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
                if not line or not line.startswith("{"):
                    continue
                try:
                    rec = json.loads(line)
                    if rec.get("company_name"):
                        existing[rec["company_name"]] = rec
                except (json.JSONDecodeError, KeyError):
                    continue
    return existing


# ── Indeed scraping ────────────────────────────────────────────────────────────

def build_indeed_url(company_name: str) -> str:
    """Build an Indeed job search URL for a company name.

    Searching without quotes lets Indeed's ranking engine surface jobs
    from that employer via its company-name index rather than doing
    a free-text match that returns unrelated results.
    """
    q = quote_plus(company_name + " jobs")
    return f"https://www.indeed.com/jobs?q={q}&l=&sort=date"


def _is_cf_challenge(html: str) -> bool:
    """Detect Cloudflare Turnstile / interstitial challenge pages."""
    head = html[:4000].lower()
    return (
        "just a moment" in head
        or "challenges.cloudflare.com/turnstile" in head
        or "challenge-platform" in head
        or "security check" in head
        or (len(html) < 120_000 and "indeed" not in head)
    )


def _is_auth_blocked(html: str) -> bool:
    """Detect Indeed login/auth redirect pages."""
    head = html[:4000].lower()
    return (
        "unusual traffic" in head
        or "verify you are human" in head
        or 'id="challenge-form"' in head
        or "<title>authenticating" in head
        or "redirecting to login" in head
    )


def extract_jobs_from_html(html: str) -> list[dict]:
    """Extract job cards from Indeed's embedded mosaic JSON blob.

    Indeed embeds all job card data in:
        window.mosaic.providerData["mosaic-provider-jobcards"] = {...};

    Fields used: title, company, formattedLocation, snippet, urgentlyHiring,
    formattedRelativeTime, extractedSalary/salarySnippet.
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

        # Salary
        salary_obj = item.get("extractedSalary") or item.get("salarySnippet")
        salary = (
            salary_obj.get("text", "") if isinstance(salary_obj, dict)
            else str(salary_obj or "")
        ).strip()

        jobs.append({
            "title": title,
            "employer": company,
            "location": location,
            "snippet": snippet,
            "posted": posted,
            "salary": salary,
            "urgent": urgent,
        })

    return jobs


def _company_name_matches(target: str, employer: str, snippet: str) -> bool:
    """Return True if a job card plausibly belongs to the target company.

    Matching logic (in order of precedence):
    1. Exact substring match between target and employer names
    2. At least one significant token (>3 chars, non-stopword) from the target
       appears in the employer name
    3. Target company name appears verbatim in the job snippet
    4. Employer field is empty (can't determine, accept)
    """
    target_lower = target.lower().strip()
    employer_lower = employer.lower().strip()
    snippet_lower = snippet.lower()

    if target_lower in employer_lower or employer_lower in target_lower:
        return True

    stopwords = {
        "inc", "corp", "llc", "ltd", "co", "company", "the", "and",
        "group", "solutions", "systems", "services", "industries",
        "manufacturing", "international", "technologies", "technology",
    }
    tokens = [
        w for w in re.split(r"[\s,./&\-()]+", target_lower)
        if len(w) > 3 and w not in stopwords
    ]
    if tokens and employer_lower and any(t in employer_lower for t in tokens[:2]):
        return True

    if target_lower in snippet_lower:
        return True

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
    """Scan jobs for ERP keyword mentions.

    Returns (primary_vendor_or_None, deduplicated_mentions_list).
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
                    pos = text.find(kw_lower, pos + 1)

    # Deduplicate (same vendor+keyword+job_title)
    seen: set[tuple] = set()
    deduped: list[dict] = []
    for m in mentions:
        key = (m["vendor"], m["keyword"], m["job_title"])
        if key not in seen:
            seen.add(key)
            deduped.append(m)

    primary = vendor_counts.most_common(1)[0][0] if vendor_counts else None
    return primary, deduped[:15]


def detect_tech(jobs: list[dict]) -> dict[str, list[str]]:
    """Scan jobs for additional tech stack signals."""
    combined = " ".join(
        (j["title"] + " " + j["snippet"]).lower()
        for j in jobs
    )
    detected: dict[str, list[str]] = {}
    for category, keywords in TECH_KEYWORDS.items():
        found = [kw for kw in keywords if kw.lower() in combined]
        if found:
            detected[category] = list(dict.fromkeys(found))
    return detected


def assess_hiring_signal(jobs: list[dict]) -> str:
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
    """Headed Playwright browser session for Indeed job lookups.

    A single browser context is maintained throughout the run so that
    Cloudflare session cookies remain valid across requests.  The CF
    challenge is solved once at startup via the warm-up step.

    If the page crashes (TargetClosedError), a new page is opened within
    the same context so the CF cookies are preserved.
    """

    _UA = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    )

    # Persistent browser profile directory — stores cookies, localStorage,
    # and browsing history across runs.  CF Turnstile treats a profile with
    # prior history as a real user, bypassing the challenge.
    _PROFILE_DIR = str(PROJECT_ROOT / "data" / "browser_profiles" / "indeed_chrome")

    def __init__(self, cf_warmup_seconds: int = 0):
        """
        Args:
            cf_warmup_seconds: Extra seconds to pause at the Indeed homepage
                before starting scrapes.  Use 45 (or pass --wait-for-cf) to
                allow time for manual CF challenge completion if needed.
        """
        from playwright.sync_api import sync_playwright
        # Ensure profile directory exists
        Path(self._PROFILE_DIR).mkdir(parents=True, exist_ok=True)

        self._pw = sync_playwright().start()
        # Use launch_persistent_context so cookies and localStorage survive
        # between Python processes.  CF Turnstile treats accumulated browsing
        # history as a trust signal and is much less aggressive.
        self._context = self._pw.chromium.launch_persistent_context(
            self._PROFILE_DIR,
            headless=False,
            viewport={"width": 1280, "height": 800},
            user_agent=self._UA,
            locale="en-US",
            timezone_id="America/New_York",
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-first-run",
                "--no-default-browser-check",
            ],
            extra_http_headers={
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;"
                    "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
                ),
            },
        )
        self._browser = None  # not used with persistent context
        self._context.add_init_script(_STEALTH_SCRIPT)
        self._page = (
            self._context.pages[0]
            if self._context.pages
            else self._context.new_page()
        )
        self._cf_ok = False
        self._warmup(cf_warmup_seconds)

    def _warmup(self, extra_seconds: int) -> None:
        """Visit Indeed homepage to establish CF session cookie.

        We wait for the `cf_clearance` cookie to be set, which signals that
        Cloudflare's Turnstile challenge has been completed.  Without this
        cookie, the /jobs endpoint returns a "Just a moment..." challenge page.

        Args:
            extra_seconds: Extra wait time for manual challenge completion.
        """
        print("  [warmup] Loading Indeed homepage to establish session...", flush=True)

        for attempt in range(2):
            try:
                self._page.goto(
                    "https://www.indeed.com/",
                    timeout=PAGE_LOAD_TIMEOUT,
                    wait_until="networkidle",
                )
                break
            except Exception:
                if attempt == 0:
                    time.sleep(3)
                    continue

        def _get_cf_clearance() -> bool:
            try:
                all_cookies = self._context.cookies()
                return any(c.get("name") == "cf_clearance" for c in all_cookies)
            except Exception:
                return False

        # Wait for cf_clearance cookie (signals CF challenge is solved)
        deadline = time.time() + max(extra_seconds, 20)
        cf_cleared = False
        while time.time() < deadline:
            if _get_cf_clearance():
                cf_cleared = True
                break
            time.sleep(2)

        if not cf_cleared and extra_seconds > 0:
            print(f"  [warmup] CF clearance cookie not found. "
                  f"Please complete the challenge in the browser window.")
            extra_deadline = time.time() + extra_seconds
            while time.time() < extra_deadline:
                remaining = int(extra_deadline - time.time())
                if _get_cf_clearance():
                    cf_cleared = True
                    print("  [warmup] CF challenge resolved!", flush=True)
                    break
                if remaining % 10 == 0:
                    print(f"  [warmup] {remaining}s remaining...", flush=True)
                time.sleep(2)

        # Visit a search result to establish /jobs-endpoint cookies
        if cf_cleared:
            try:
                self._page.goto(
                    "https://www.indeed.com/jobs?q=manufacturing&l=&sort=date",
                    timeout=PAGE_LOAD_TIMEOUT,
                    wait_until="domcontentloaded",
                )
                time.sleep(3)
            except Exception:
                pass

        try:
            html = self._page.content()
            self._cf_ok = (
                cf_cleared
                and not _is_cf_challenge(html)
                and len(html) > 100_000
            )
        except Exception:
            self._cf_ok = False

        if self._cf_ok:
            print("  [warmup] Session established successfully.", flush=True)
        else:
            print(
                "  [warmup] WARNING: CF challenge may not be fully resolved. "
                "Run with --wait-for-cf if jobs return 0 results.",
                flush=True,
            )

    def _ensure_page(self) -> None:
        """Reopen the page if it was closed mid-navigation."""
        try:
            _ = self._page.url
        except Exception:
            self._page = self._context.new_page()

    def close(self) -> None:
        try:
            self._context.close()
        except Exception:
            pass
        try:
            self._pw.stop()
        except Exception:
            pass

    def fetch(self, url: str) -> tuple[str | None, str]:
        """Fetch *url* and return (html_or_None, status_tag).

        status_tag values:
            'ok'      — page loaded, mosaic JSON may or may not contain jobs
            'cf'      — Cloudflare challenge not resolved
            'blocked' — Indeed auth/bot block
            'error'   — navigation exception
        """
        self._ensure_page()

        for attempt in range(2):
            try:
                self._page.goto(url, timeout=PAGE_LOAD_TIMEOUT, wait_until="domcontentloaded")
                break
            except Exception as e:
                err_msg = str(e).lower()
                if attempt == 0 and (
                    "target" in err_msg or "closed" in err_msg
                    or "crash" in err_msg or "navigation" in err_msg
                ):
                    # Page was killed mid-navigation; open a new one
                    try:
                        self._page = self._context.new_page()
                    except Exception:
                        return None, "error"
                    time.sleep(3)
                    continue
                return None, "error"

        # Handle CF challenge that didn't auto-solve
        try:
            html_check = self._page.content()
        except Exception:
            return None, "error"

        if _is_cf_challenge(html_check):
            # Wait up to CF_WAIT_TIMEOUT for auto-resolution
            deadline = time.time() + CF_WAIT_TIMEOUT / 1000
            while time.time() < deadline:
                time.sleep(3)
                try:
                    html_check = self._page.content()
                    if not _is_cf_challenge(html_check):
                        break
                except Exception:
                    return None, "error"
            else:
                return None, "cf"

        # Wait for mosaic job data injection
        try:
            self._page.wait_for_function(
                "() => !!(window.mosaic && window.mosaic.providerData)",
                timeout=SELECTOR_TIMEOUT,
            )
        except Exception:
            pass  # pages with 0 results may have no mosaic

        try:
            html = self._page.content()
        except Exception:
            return None, "error"

        if _is_auth_blocked(html):
            return None, "blocked"

        return html, "ok"

    def scrape_company(self, company: dict) -> dict:
        """Scrape Indeed for one company and return a job intelligence record."""
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
            "indeed_status": "unknown",
            "scraped_at": now,
        }

        html, status = self.fetch(url)
        base_result["indeed_status"] = status

        if status == "cf":
            print(f"      [CF blocked] backing off {CAPTCHA_BACKOFF}s", flush=True)
            time.sleep(CAPTCHA_BACKOFF)
            html, status = self.fetch(url)
            base_result["indeed_status"] = status

        if html is None:
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
    signal = result["hiring_signal"]
    status = result.get("indeed_status", "")
    tech_cats = ", ".join(result.get("tech_detected", {}).keys()) or "-"
    erp_tag = f"  ERP={result['erp_detected']}" if result["erp_detected"] else ""
    status_tag = f"  [{status}]" if status not in ("ok", "unknown") else ""
    print(
        f"  {name}  jobs={jobs:<3}  signal={signal:<6}  "
        f"tech=[{tech_cats[:28]}]{erp_tag}{status_tag}",
        flush=True,
    )


def print_summary(results: list[dict]) -> None:
    total = len(results)
    if total == 0:
        return

    with_jobs = sum(1 for r in results if r["job_count"] > 0)
    erp_list = [r for r in results if r["erp_detected"]]
    all_erps: Counter = Counter(r["erp_detected"] for r in erp_list)
    signals: Counter = Counter(r["hiring_signal"] for r in results)
    statuses: Counter = Counter(r.get("indeed_status", "?") for r in results)

    all_tech: Counter = Counter()
    for r in results:
        for cat in r.get("tech_detected", {}):
            all_tech[cat] += 1

    print()
    print("=" * 65)
    print("Job Intelligence — Summary")
    print("=" * 65)
    print(f"  Companies processed:    {total}")
    print(f"  With job listings:      {with_jobs} ({with_jobs/total*100:.1f}%)")
    print(f"  ERP confirmed:          {len(erp_list)} ({len(erp_list)/total*100:.1f}%)")

    print()
    print("  Hiring Signal Distribution:")
    for sig, cnt in signals.most_common():
        print(f"    {sig:<10}  {cnt}")

    print()
    print("  Indeed Response Statuses:")
    for st, cnt in statuses.most_common():
        print(f"    {st:<10}  {cnt}")

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
        help="Skip companies already in the output file",
    )
    parser.add_argument(
        "--wait-for-cf",
        action="store_true",
        dest="wait_for_cf",
        help=(
            "Pause 45s at startup so you can manually complete the "
            "Cloudflare challenge in the browser window"
        ),
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

    cf_warmup = 45 if args.wait_for_cf else 0
    scraper = IndeedScraper(cf_warmup_seconds=cf_warmup)
    print()

    try:
        with open(OUTPUT_PATH, out_mode, encoding="utf-8") as out_f:
            for i, company in enumerate(to_process):
                result = scraper.scrape_company(company)
                results_this_run.append(result)

                # Crash-safe incremental write
                out_f.write(json.dumps(result, ensure_ascii=False) + "\n")
                out_f.flush()

                print_result(result)

                if (i + 1) % 25 == 0:
                    erp_so_far = sum(1 for r in results_this_run if r["erp_detected"])
                    jobs_so_far = sum(1 for r in results_this_run if r["job_count"] > 0)
                    cf_blocks = sum(1 for r in results_this_run
                                    if r.get("indeed_status") == "cf")
                    print(
                        f"\n  --- Progress: {i+1}/{len(to_process)} | "
                        f"with jobs: {jobs_so_far} | ERP: {erp_so_far} | "
                        f"CF blocks: {cf_blocks} ---\n",
                        flush=True,
                    )

                if i < len(to_process) - 1:
                    time.sleep(RATE_LIMIT_DELAY)

    finally:
        scraper.close()

    print_summary(results_this_run)


if __name__ == "__main__":
    main()
