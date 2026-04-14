"""
AFS Castingsource metalcaster directory extractor.
Navigates https://www.castingsource.com/metalcaster-directory,
selects United States, paginates through all results (pages 0-13),
and writes records to data/raw/AFS/records.jsonl.

Page structure (Drupal Views):
  - URL: /metalcaster-directory?country=US&page=N
  - Each row: .views-field-Company-Name__c .company-name   -> company name
  - Address:  .views-field-Country__c .field-content       -> "STREET\nCITY, ST ZIP, US"
  - Website:  .views-field-Org-URL__c .field-content a     -> URL
  - Pager:    .pager__items (last link is "page=13")
"""

import json
import re
import time
import datetime
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError

OUTPUT_PATH = Path(__file__).parent.parent / "data" / "raw" / "AFS" / "records.jsonl"
INSPECT_DIR = Path(__file__).parent.parent / "data" / "raw" / "AFS"
BASE_URL = "https://www.castingsource.com/metalcaster-directory"
ASSOCIATION = "AFS"
COUNTRY = "United States"

# Address format: "STREET\nCITY, ST ZIP, US"
# The regex must account for optional ", US" suffix
ADDR_RE = re.compile(
    r"^(.+?),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)(?:,\s*US)?$"
)


def extract_domain(url: str) -> str:
    """Strip protocol and www. prefix to get bare domain."""
    if not url:
        return ""
    url = url.strip()
    try:
        # Use basic parsing
        url = re.sub(r"^https?://", "", url, flags=re.IGNORECASE)
        url = re.sub(r"^www\.", "", url, flags=re.IGNORECASE)
        url = url.split("/")[0].split("?")[0].split("#")[0]
        return url.lower().strip()
    except Exception:
        return ""


def parse_address(raw_addr: str) -> tuple[str, str, str, str]:
    """
    Parse raw address block into (street, city, state, zip).
    Input: "3535 Waynesboro Hwy\nLawrenceburg, TN 38464, US"
    """
    if not raw_addr:
        return "", "", "", ""

    lines = [l.strip() for l in raw_addr.strip().split("\n") if l.strip()]

    if len(lines) == 0:
        return "", "", "", ""

    if len(lines) == 1:
        # Single line: might be just "CITY, ST ZIP"
        m = ADDR_RE.match(lines[0])
        if m:
            return "", m.group(1).strip(), m.group(2).strip(), m.group(3).strip()
        return lines[0], "", "", ""

    # Multi-line: last line is city/state/zip, everything before is street
    city_line = lines[-1]
    street = " ".join(lines[:-1])

    m = ADDR_RE.match(city_line)
    if m:
        return street, m.group(1).strip(), m.group(2).strip(), m.group(3).strip()

    # Fallback: return street only
    return street, "", "", ""


def extract_companies_from_page(page, source_url: str) -> list[dict]:
    """
    Extract all company rows from the current results page.
    Targets the metalcaster_directory view (NOT the process search view).
    """
    extracted_at = datetime.datetime.now(datetime.UTC).isoformat()

    raw_records = page.evaluate("""
        () => {
            const results = [];

            // Target specifically the metalcaster_directory view, not the process search
            // The metalcaster directory view has id containing "metalcaster_directory"
            // and view-display-id-page_1
            let viewContent = null;

            // Look for the correct view by examining all view-content divs
            const allViews = document.querySelectorAll('.view');
            for (const v of allViews) {
                if (v.classList.contains('view-metalcaster-directory') ||
                    v.id && v.id.includes('metalcaster_directory')) {
                    viewContent = v.querySelector('.view-content');
                    break;
                }
            }

            // Fallback: first .view-content
            if (!viewContent) {
                viewContent = document.querySelector('.view-content');
            }

            if (!viewContent) return results;

            const rows = viewContent.querySelectorAll('.views-row');

            for (const row of rows) {
                const rec = {
                    company_name: '',
                    raw_address: '',
                    website: '',
                };

                // Company name: span.company-name inside views-field-Company-Name__c
                const nameEl = row.querySelector('.views-field-Company-Name__c .company-name');
                if (nameEl) {
                    rec.company_name = nameEl.textContent.trim();
                } else {
                    // Fallback: first field content
                    const fc = row.querySelector('.views-field-Company-Name__c .field-content');
                    if (fc) {
                        // Strip "(AFS Corporate Member)" suffix
                        rec.company_name = fc.textContent.replace(/\\(AFS.*?\\)/gi, '').trim();
                    }
                }

                // Address: views-field-Country__c (misnamed field, but contains address)
                const addrEl = row.querySelector('.views-field-Country__c .field-content');
                if (addrEl) {
                    rec.raw_address = addrEl.textContent.trim();
                }

                // Website: views-field-Org-URL__c
                const urlField = row.querySelector('.views-field-Org-URL__c .field-content');
                if (urlField) {
                    const link = urlField.querySelector('a');
                    if (link) {
                        rec.website = link.href;
                    } else {
                        // Plain text URL
                        const txt = urlField.textContent.trim();
                        if (txt && (txt.startsWith('http') || txt.includes('.'))) {
                            rec.website = txt;
                        }
                    }
                }

                // Only include rows that have a company name and look like real company records
                // (filter out process search noise)
                if (rec.company_name && rec.company_name.length > 1 &&
                    !rec.company_name.startsWith('Alloy:') &&
                    !rec.company_name.includes('Expected Tolerance') &&
                    !rec.company_name.includes('Process:')) {
                    results.push(rec);
                }
            }

            return results;
        }
    """)

    # Now parse addresses in Python
    records = []
    for raw in raw_records:
        street, city, state, zip_code = parse_address(raw.get("raw_address", ""))

        website = raw.get("website", "").strip()
        # Normalize website: ensure it has protocol
        if website and not website.startswith("http"):
            website = "http://" + website

        domain = extract_domain(website) if website else ""

        record = {
            "company_name": raw["company_name"],
            "street": street,
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "website": website,
            "domain": domain,
            "association": ASSOCIATION,
            "source_url": source_url,
            "extracted_at": extracted_at,
            "country": COUNTRY,
        }
        records.append(record)

    return records


def get_last_page_number(page) -> int:
    """Find the last page number from the pager."""
    result = page.evaluate("""
        () => {
            const items = document.querySelectorAll('.pager__items li a');
            let maxPage = 0;
            for (const a of items) {
                const href = a.href || '';
                const m = href.match(/page=(\\d+)/);
                if (m) {
                    const n = parseInt(m[1], 10);
                    if (n > maxPage) maxPage = n;
                }
            }
            return maxPage;
        }
    """)
    return result if isinstance(result, int) else 13  # default to 13 if not found


def run_extraction():
    """Full extraction across all pages."""
    all_records = []

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 900},
        )
        page = context.new_page()

        # ── Load page 0 to find total pages ──
        url_page0 = f"{BASE_URL}?country=US"
        print(f"\nLoading page 0: {url_page0}")
        page.goto(url_page0, wait_until="domcontentloaded", timeout=60000)
        time.sleep(3)

        last_page = get_last_page_number(page)
        print(f"Last page number detected: {last_page} ({last_page + 1} total pages)")

        # Extract page 0
        records = extract_companies_from_page(page, url_page0)
        print(f"Page 0: {len(records)} companies")
        if records:
            print(f"  First: {records[0]['company_name']} | {records[0]['street']} | {records[0]['city']}, {records[0]['state']} {records[0]['zip_code']}")
            print(f"  Last:  {records[-1]['company_name']} | {records[-1]['city']}, {records[-1]['state']}")
        all_records.extend(records)

        # ── Pages 1 through last_page ──
        for page_num in range(1, last_page + 1):
            url = f"{BASE_URL}?country=US&page={page_num}"
            print(f"\nLoading page {page_num}: {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(2)

            records = extract_companies_from_page(page, url)
            print(f"Page {page_num}: {len(records)} companies")
            if records:
                print(f"  First: {records[0]['company_name']} | {records[0]['city']}, {records[0]['state']}")
                print(f"  Last:  {records[-1]['company_name']} | {records[-1]['city']}, {records[-1]['state']}")
            all_records.extend(records)

            # Checkpoint every 5 pages
            if page_num % 5 == 0:
                _save_records(all_records)
                print(f"  Checkpoint: {len(all_records)} total records")

        browser.close()

    # Deduplicate by company_name + city + state
    seen = set()
    deduped = []
    for rec in all_records:
        key = (rec["company_name"].lower().strip(), rec["city"].lower().strip(), rec["state"].lower().strip())
        if key not in seen:
            seen.add(key)
            deduped.append(rec)

    print(f"\nRaw: {len(all_records)}, After dedup: {len(deduped)}")
    return deduped


def _save_records(records: list[dict]) -> None:
    """Write all records to JSONL output file."""
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")
    print(f"  Saved {len(records)} records to {OUTPUT_PATH}")


if __name__ == "__main__":
    print("=== AFS Castingsource Extractor ===")
    records = run_extraction()

    if records:
        _save_records(records)
        print(f"\n=== EXTRACTION COMPLETE ===")
        print(f"Total unique companies: {len(records)}")

        # Stats
        states: dict[str, int] = {}
        for r in records:
            s = r.get("state", "") or "unknown"
            states[s] = states.get(s, 0) + 1
        top_states = sorted(states.items(), key=lambda x: -x[1])[:10]
        print(f"Top states: {top_states}")

        with_website = sum(1 for r in records if r.get("website"))
        with_city = sum(1 for r in records if r.get("city"))
        print(f"With website: {with_website}/{len(records)}")
        print(f"With city: {with_city}/{len(records)}")

        print(f"\nSample records:")
        for rec in records[:5]:
            print(f"  {rec['company_name']:50s} | {rec['street'][:30]:30s} | {rec['city']}, {rec['state']} {rec['zip_code']} | {rec['website'][:40] if rec['website'] else 'no website'}")
    else:
        print("\nNo records extracted!")
