"""
PMA Profile Scraper using httpx with Cloudflare cookies from MCP Playwright.

Much faster than Playwright-per-page since we reuse the CF clearance token.
Uses the same extraction logic but via regex on raw HTML instead of JS evaluate.

Usage:
    python scripts/scrape_pma_httpx.py [--limit N] [--resume]
"""

import asyncio
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_smoke_test.jsonl"
OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_enriched.jsonl"
COOKIES_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "cf_cookies.json"

DELAY = 1.5  # seconds between requests

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/131.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.pma.org/directory/",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


def extract_domain(url):
    if not url:
        return ""
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    try:
        d = urlparse(url).netloc.lower()
        return d.removeprefix("www.")
    except Exception:
        return ""


def parse_profile_html(html: str) -> dict | None:
    """Parse PMA profile page HTML and extract company data."""
    # Check for "no record found"
    if "Sorry, no record found" in html or "is not an active member" in html:
        return None

    # Check we have an article
    article_match = re.search(r'<article[^>]*>(.*?)</article>', html, re.DOTALL)
    if not article_match:
        return None

    article = article_match.group(1)
    result = {}

    # Company name: second h2 in article
    h2s = re.findall(r'<h2[^>]*>(.*?)</h2>', article, re.DOTALL)
    if len(h2s) >= 2:
        result["companyName"] = re.sub(r'<[^>]+>', '', h2s[1]).strip()
    elif len(h2s) == 1:
        name = re.sub(r'<[^>]+>', '', h2s[0]).strip()
        if name not in ("Member Profile", "Sorry, no record found"):
            result["companyName"] = name

    if not result.get("companyName"):
        return None

    # Address block
    addr_match = re.search(
        r'class="company-address"[^>]*>(.*?)</div>',
        article, re.DOTALL
    )
    if addr_match:
        addr_text = re.sub(r'<[^>]+>', '\n', addr_match.group(1))
        lines = [l.strip() for l in addr_text.split('\n')
                 if l.strip() and l.strip() != 'Map Location']
        if lines:
            result["street"] = lines[0]
        if len(lines) > 1:
            m = re.match(r'^(.+?),\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$', lines[1])
            if m:
                result["city"] = m.group(1)
                result["state"] = m.group(2)
                result["zip"] = m.group(3)
        if len(lines) > 2:
            result["country"] = lines[2]

    # Contact block (phone, fax, website)
    contact_match = re.search(
        r'class="company-contact"[^>]*>(.*?)</div>',
        article, re.DOTALL
    )
    if contact_match:
        contact_text = contact_match.group(1)
        phone_m = re.search(r'Phone:</strong>\s*([\d\(\)\s\-\+\.]+)', contact_text)
        if phone_m:
            result["phone"] = phone_m.group(1).strip()
        fax_m = re.search(r'Fax:</strong>\s*([\d\(\)\s\-\+\.]+)', contact_text)
        if fax_m:
            result["fax"] = fax_m.group(1).strip()
        # Website link
        web_m = re.search(r'/go\.asp\?url=([^"\']+)', contact_text)
        if web_m:
            result["website"] = web_m.group(1)
        else:
            # Try direct link
            link_m = re.search(r'<a[^>]+href="(https?://[^"]+)"', contact_text)
            if link_m:
                href = link_m.group(1)
                if 'pma.org' not in href and 'maps.google' not in href:
                    result["website"] = href

    # Facility block
    facility_match = re.search(
        r'class="company-facility"[^>]*>(.*?)</div>',
        article, re.DOTALL
    )
    if facility_match:
        fac_text = facility_match.group(1)
        emp_m = re.search(r'Employees:\s*([^<\n]+)', fac_text)
        if emp_m:
            result["employees"] = emp_m.group(1).strip()
        size_m = re.search(r'Facility Size:\s*([^<\n]+)', fac_text)
        if size_m:
            result["facilitySize"] = size_m.group(1).strip()

    # Membership level
    level_match = re.search(
        r'class="company-level"[^>]*>(.*?)</div>',
        article, re.DOTALL
    )
    if level_match:
        level_text = level_match.group(1)
        tier_m = re.search(
            r'(PLATINUM|GOLD|SILVER|BRONZE|PREMIER)\s+MEMBER',
            level_text, re.IGNORECASE
        )
        if tier_m:
            result["tier"] = tier_m.group(1).upper()
        since_m = re.search(r'Member Since:.*?(\d{4})', level_text)
        if since_m:
            result["memberSince"] = since_m.group(1)

    # List sections (processes, certifications, markets)
    def get_list_section(heading):
        items = []
        pattern = (
            r'<h3[^>]*>[^<]*' + re.escape(heading) +
            r'[^<]*</h3>(.*?)(?=<h3|<hr|</article)'
        )
        match = re.search(pattern, article, re.DOTALL)
        if match:
            # Find div items with style attribute
            divs = re.findall(r'<div\s+style[^>]*>(.*?)</div>', match.group(1), re.DOTALL)
            for div_content in divs:
                text = re.sub(r'<[^>]+>', '', div_content).strip()
                text = re.sub(r'^[\s\xa0]+', '', text)
                if text:
                    items.append(text)
        return items

    result["processes"] = get_list_section("Manufacturing Processes")
    result["certifications"] = get_list_section("Certifications")
    result["markets"] = get_list_section("Markets Served")

    # Company description
    desc_match = re.search(
        r'Company Description</[^>]+>(.*?)(?=<h3|<hr|</article|class="member-search-blocks")',
        article, re.DOTALL
    )
    if desc_match:
        desc = re.sub(r'<[^>]+>', ' ', desc_match.group(1)).strip()
        desc = re.sub(r'\s+', ' ', desc)
        result["description"] = desc

    # Contacts from HTML comments
    contacts = []
    comment_pattern = re.compile(r'<!--(.*?)-->', re.DOTALL)
    for cm in comment_pattern.finditer(html):
        comment = cm.group(1)
        if 'contact-info' not in comment:
            continue
        blocks = comment.split('<div class="contact-info"')
        for block in blocks[1:]:
            contact = {}
            nm = re.search(r'<strong>(.*?)</strong>', block)
            if nm:
                contact["name"] = nm.group(1).strip()
            title_m = re.search(
                r'</strong>\s*<br>\s*\n?\s*(.+?)\s*<br>', block
            )
            if title_m:
                t = title_m.group(1).strip()
                if not any(t.startswith(x) for x in ('Phone', 'Fax', 'Email', '<')):
                    contact["title"] = t
            ph_m = re.search(r'Phone:\s*(?:&nbsp;)?\s*([^<\n]+)', block)
            if ph_m:
                contact["phone"] = ph_m.group(1).strip()
            em_m = re.search(
                r'<a\s+class="no-mail"\s+name="([^"]+)"\s+rel="([^"]+)"',
                block
            )
            if em_m:
                contact["email"] = em_m.group(1) + '@' + em_m.group(2).replace('#', '.')
            if contact.get("name"):
                contacts.append(contact)
    result["contacts"] = contacts

    return result


def merge_profile(record: dict, profile: dict) -> dict:
    """Merge scraped profile data into original record."""
    enriched = dict(record)
    enriched["enriched_at"] = datetime.now(timezone.utc).isoformat()

    if profile.get("companyName"):
        enriched["company_name"] = profile["companyName"]
    if profile.get("website"):
        enriched["website"] = profile["website"]
        enriched["domain"] = extract_domain(profile["website"])
    if profile.get("phone"):
        enriched["phone"] = profile["phone"]
    if profile.get("fax"):
        enriched["fax"] = profile["fax"]
    if profile.get("street"):
        enriched["street"] = profile["street"]
    if profile.get("zip"):
        enriched["zip_code"] = profile["zip"]
    if profile.get("city"):
        enriched["city"] = profile["city"]
    if profile.get("state"):
        enriched["state"] = profile["state"]
    if profile.get("country"):
        enriched["country"] = profile["country"]
    if profile.get("employees"):
        enriched["employees"] = profile["employees"]
        emp = profile["employees"]
        m = re.match(r"(\d[\d,]*)\s*-\s*(\d[\d,]*)", emp)
        if m:
            enriched["employee_count_min"] = int(m.group(1).replace(",", ""))
            enriched["employee_count_max"] = int(m.group(2).replace(",", ""))
        elif "+" in emp:
            num = re.search(r"(\d[\d,]*)", emp)
            if num:
                enriched["employee_count_min"] = int(num.group(1).replace(",", ""))
    if profile.get("facilitySize"):
        enriched["facility_size_sqft"] = profile["facilitySize"]
    if profile.get("tier"):
        enriched["membership_tier"] = profile["tier"]
    if profile.get("memberSince"):
        enriched["member_since"] = profile["memberSince"]
    if profile.get("processes"):
        enriched["manufacturing_processes"] = profile["processes"]
    if profile.get("certifications"):
        enriched["certifications"] = profile["certifications"]
    if profile.get("markets"):
        enriched["markets_served"] = profile["markets"]
    if profile.get("description"):
        enriched["description"] = profile["description"]
    if profile.get("contacts"):
        enriched["contacts"] = profile["contacts"]

    enriched["enrichment_status"] = "scraped"
    enriched["source_url"] = record.get("profile_url", "")
    enriched["extracted_at"] = datetime.now(timezone.utc).isoformat()
    return enriched


def load_records():
    records = []
    with open(INPUT_FILE, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line.strip()))
    return records


def load_already_scraped():
    scraped = {}
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rec = json.loads(line.strip())
                    mid = rec.get("member_id", "")
                    if mid and rec.get("enriched_at"):
                        scraped[mid] = rec
    return scraped


def load_cookies() -> dict:
    """Load CF cookies from file (exported by MCP Playwright)."""
    if not COOKIES_FILE.exists():
        return {}
    with open(COOKIES_FILE) as f:
        cookie_list = json.load(f)
    cookies = {}
    for c in cookie_list:
        cookies[c["name"]] = c["value"]
    return cookies


async def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    records = load_records()
    print(f"Loaded {len(records)} PMA records")

    # Deduplicate by member_id
    seen = set()
    unique = []
    for r in records:
        mid = r.get("member_id", "")
        if mid and mid not in seen:
            seen.add(mid)
            unique.append(r)
    print(f"Unique by member_id: {len(unique)}")

    # Resume support
    already = load_already_scraped() if args.resume else {}
    to_scrape = [r for r in unique if r.get("member_id") not in already]
    if args.limit > 0:
        to_scrape = to_scrape[:args.limit]
    print(f"Already scraped: {len(already)}, to scrape: {len(to_scrape)}")

    if not to_scrape:
        print("Nothing to scrape!")
        return

    # Load Cloudflare cookies
    cookies = load_cookies()
    if not cookies.get("cf_clearance"):
        print("WARNING: No cf_clearance cookie found. Export cookies from MCP Playwright first.")
        print("  Run: python -c \"import json; ...\" to export cookies")

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    stats = {"scraped": 0, "websites": 0, "phones": 0, "contacts": 0,
             "emails": 0, "errors": 0, "not_found": 0}
    all_enriched = list(already.values())  # Start with previously scraped

    start_time = time.time()

    async with httpx.AsyncClient(
        headers=HEADERS,
        cookies=cookies,
        follow_redirects=True,
        timeout=15,
    ) as client:
        for i, record in enumerate(to_scrape):
            url = record.get("profile_url", "")
            company = record.get("company_name", "Unknown")
            mid = record.get("member_id", "")

            if not url:
                all_enriched.append(record)
                continue

            pct = (i + 1) / len(to_scrape) * 100
            print(f"[{i+1}/{len(to_scrape)} {pct:.0f}%] {company}...", end=" ", flush=True)

            try:
                resp = await client.get(url)
                html = resp.text
                status = resp.status_code

                if status == 403:
                    print(f"CF_BLOCKED (403)")
                    stats["errors"] += 1
                    all_enriched.append(record)
                    # If we get blocked, pause longer
                    await asyncio.sleep(5)
                    continue

                if status != 200:
                    print(f"HTTP_{status}")
                    stats["errors"] += 1
                    all_enriched.append(record)
                    continue

                profile = parse_profile_html(html)

                if profile:
                    enriched = merge_profile(record, profile)
                    all_enriched.append(enriched)
                    stats["scraped"] += 1
                    if enriched.get("website"):
                        stats["websites"] += 1
                    if enriched.get("phone"):
                        stats["phones"] += 1
                    if enriched.get("contacts"):
                        stats["contacts"] += 1
                        stats["emails"] += sum(
                            1 for c in enriched["contacts"] if c.get("email")
                        )
                    web = "Y" if enriched.get("website") else "N"
                    ph = "Y" if enriched.get("phone") else "N"
                    nc = len(enriched.get("contacts", []))
                    print(f"OK web={web} ph={ph} contacts={nc}")
                else:
                    print("NOT_FOUND")
                    stats["not_found"] += 1
                    all_enriched.append(record)

            except Exception as e:
                print(f"ERR: {str(e)[:60]}")
                stats["errors"] += 1
                all_enriched.append(record)

            # Save progress every 50 records
            if (i + 1) % 50 == 0:
                with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                    for r in all_enriched:
                        f.write(json.dumps(r, ensure_ascii=False) + "\n")
                print(f"  [Checkpoint: {len(all_enriched)} records saved]")

            # Rate limit
            if i < len(to_scrape) - 1:
                await asyncio.sleep(DELAY)

    # Final save
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for r in all_enriched:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    total_time = time.time() - start_time
    total = stats["scraped"] + stats["errors"] + stats["not_found"]
    print(f"\n{'='*60}")
    print(f"PMA Profile Scraping Complete (httpx)")
    print(f"{'='*60}")
    print(f"Scraped:     {stats['scraped']}/{total}")
    print(f"Websites:    {stats['websites']} ({stats['websites']/max(stats['scraped'],1)*100:.0f}%)")
    print(f"Phones:      {stats['phones']} ({stats['phones']/max(stats['scraped'],1)*100:.0f}%)")
    print(f"Contacts:    {stats['contacts']} records")
    print(f"Emails:      {stats['emails']} total")
    print(f"Not found:   {stats['not_found']}")
    print(f"Errors:      {stats['errors']}")
    print(f"Time:        {total_time/60:.1f} min")
    print(f"Output:      {OUTPUT_FILE} ({len(all_enriched)} records)")


if __name__ == "__main__":
    asyncio.run(main())
