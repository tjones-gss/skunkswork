"""
PMA Profile Batch Scraper using local Playwright.

Scrapes all 1,064 PMA profile pages for website, phone, address,
contacts, employee count, membership tier, and manufacturing processes.

Usage:
    python scripts/scrape_pma_batch.py [--limit N] [--resume]
"""

import asyncio
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from playwright.async_api import async_playwright

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_smoke_test.jsonl"
OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_enriched.jsonl"

DELAY = 1.5  # seconds between requests

_STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = {runtime: {}};
"""

EXTRACT_JS = """() => {
  const article = document.querySelector('article');
  if (!article) return null;
  const h2 = article.querySelectorAll('h2');
  const companyName = h2.length > 1 ? h2[1].textContent.trim() : '';
  const addressDiv = article.querySelector('.company-address');
  let street='',city='',state='',zip='',country='';
  if(addressDiv){const text=addressDiv.innerText;const lines=text.split('\\n').map(l=>l.trim()).filter(l=>l&&l!=='Map Location');if(lines[0])street=lines[0];if(lines[1]){const m=lines[1].match(/^(.+?),\\s+([A-Z]{2})\\s+(\\d{5}(?:-\\d{4})?)$/);if(m){city=m[1];state=m[2];zip=m[3]}}if(lines[2])country=lines[2]}
  const contactDiv=article.querySelector('.company-contact');let phone='',fax='',website='';
  if(contactDiv){const text=contactDiv.innerText;const pm=text.match(/Phone:\\s*([^\\n]+)/);if(pm)phone=pm[1].trim();const fm=text.match(/Fax:\\s*([^\\n]+)/);if(fm)fax=fm[1].trim();const link=contactDiv.querySelector('a[href]');if(link){const href=link.getAttribute('href');if(href.includes('/go.asp?url='))website=href.split('/go.asp?url=')[1];else if(href.startsWith('http'))website=href;else{const lt=link.textContent.trim();if(lt&&lt.includes('.'))website='http://'+lt}}}
  const facilityDiv=article.querySelector('.company-facility');let employees='',facilitySize='';
  if(facilityDiv){const text=facilityDiv.innerText;const em=text.match(/Employees:\\s*([^\\n]+)/);if(em)employees=em[1].trim();const sm=text.match(/Facility Size:\\s*([^\\n]+)/);if(sm)facilitySize=sm[1].trim()}
  const levelDiv=article.querySelector('.company-level');let tier='',memberSince='';
  if(levelDiv){const text=levelDiv.innerText;const tm=text.match(/(PLATINUM|GOLD|SILVER|BRONZE|PREMIER)\\s+MEMBER/i);if(tm)tier=tm[1].toUpperCase();const sm2=text.match(/Member Since:\\s*(\\d{4})/);if(sm2)memberSince=sm2[1]}
  function getListSection(heading){const items=[];article.querySelectorAll('h3').forEach(h3=>{if(h3.textContent.includes(heading)){const parent=h3.closest('.member-search-blocks');if(parent)parent.querySelectorAll('div[style]').forEach(d=>{const t=d.textContent.trim().replace(/^[\\s\\u00a0]+/,'');if(t)items.push(t)})}});return items}
  const processes=getListSection('Manufacturing Processes');const certifications=getListSection('Certifications');const markets=getListSection('Markets Served');
  let description='';article.querySelectorAll('h3').forEach(h3=>{if(h3.textContent.includes('Company Description')){const parent=h3.closest('.member-search-blocks');if(parent)description=parent.textContent.replace('Company Description','').trim()}});
  const contacts=[];const html=document.documentElement.innerHTML;const commentPattern=/<!--([\\s\\S]*?)-->/g;let cm;
  while((cm=commentPattern.exec(html))!==null){if(!cm[1].includes('contact-info'))continue;const blocks=cm[1].split('<div class="contact-info"');for(let j=1;j<blocks.length;j++){const block=blocks[j];const contact={};const nm=block.match(/<strong>(.*?)<\\/strong>/);if(nm)contact.name=nm[1].trim();const titleM=block.match(/<\\/strong>\\s*<br>\\s*\\n?\\s*(.+?)\\s*<br>/);if(titleM){const t=titleM[1].trim();if(!t.startsWith('Phone')&&!t.startsWith('Fax')&&!t.startsWith('Email')&&!t.startsWith('<'))contact.title=t}const phM=block.match(/Phone:\\s*(?:&nbsp;)?\\s*([^<\\n]+)/);if(phM)contact.phone=phM[1].trim();const emM=block.match(/<a\\s+class="no-mail"\\s+name="([^"]+)"\\s+rel="([^"]+)"/);if(emM)contact.email=emM[1]+'@'+emM[2].replace('#','.');if(contact.name)contacts.push(contact)}}
  return{companyName,street,city,state,zip,country,phone,fax,website,employees,facilitySize,tier,memberSince,processes,certifications,markets,description,contacts};
}"""


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


def merge_profile(record, profile):
    enriched = dict(record)
    enriched["enriched_at"] = datetime.now(timezone.utc).isoformat()
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
    return enriched


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

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    stats = {"scraped": 0, "websites": 0, "phones": 0, "contacts": 0,
             "emails": 0, "errors": 0}
    all_enriched = list(already.values())  # Start with previously scraped

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        await ctx.add_init_script(_STEALTH_SCRIPT)
        page = await ctx.new_page()

        # Establish session
        print("Establishing PMA session...")
        await page.goto("https://www.pma.org/directory/", wait_until="domcontentloaded", timeout=60000)
        await asyncio.sleep(8)

        # Visit first profile
        first_url = to_scrape[0].get("profile_url", "")
        if first_url:
            await page.goto(first_url, wait_until="domcontentloaded", timeout=30000)
            try:
                await page.wait_for_selector("article", timeout=10000)
                print("Session established!")
            except Exception:
                print("WARNING: May need to solve Cloudflare challenge manually")
                await asyncio.sleep(10)

        start_time = time.time()

        for i, record in enumerate(to_scrape):
            url = record.get("profile_url", "")
            company = record.get("company_name", "Unknown")
            mid = record.get("member_id", "")

            if not url:
                all_enriched.append(record)
                continue

            pct = (i + 1) / len(to_scrape) * 100
            elapsed = time.time() - start_time
            rate = (i + 1) / max(elapsed, 1) * 3600
            print(f"[{i+1}/{len(to_scrape)} {pct:.0f}%] {company}...", end=" ", flush=True)

            try:
                resp = await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                status = resp.status if resp else 0

                if status == 403:
                    await asyncio.sleep(5)
                    try:
                        await page.wait_for_selector("article", timeout=8000)
                    except Exception:
                        await page.reload(wait_until="domcontentloaded", timeout=15000)
                        await asyncio.sleep(3)

                try:
                    await page.wait_for_selector("article", timeout=5000)
                except Exception:
                    print("NO_ARTICLE")
                    stats["errors"] += 1
                    all_enriched.append(record)
                    continue

                profile = await page.evaluate(EXTRACT_JS)

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
                        stats["emails"] += sum(1 for c in enriched["contacts"] if c.get("email"))
                    web = "Y" if enriched.get("website") else "N"
                    ph = "Y" if enriched.get("phone") else "N"
                    nc = len(enriched.get("contacts", []))
                    print(f"OK web={web} ph={ph} contacts={nc}")
                else:
                    print("NULL")
                    stats["errors"] += 1
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
                print(f"  [Checkpoint saved: {len(all_enriched)} records]")

            # Rate limit
            if i < len(to_scrape) - 1:
                await asyncio.sleep(DELAY)

        await browser.close()

    # Final save
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for r in all_enriched:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    total_time = time.time() - start_time
    total = stats["scraped"] + stats["errors"]
    print(f"\n{'='*60}")
    print(f"PMA Profile Scraping Complete")
    print(f"{'='*60}")
    print(f"Scraped:     {stats['scraped']}/{total}")
    print(f"Websites:    {stats['websites']} ({stats['websites']/max(stats['scraped'],1)*100:.0f}%)")
    print(f"Phones:      {stats['phones']} ({stats['phones']/max(stats['scraped'],1)*100:.0f}%)")
    print(f"Contacts:    {stats['contacts']} records")
    print(f"Emails:      {stats['emails']} total")
    print(f"Errors:      {stats['errors']}")
    print(f"Time:        {total_time/60:.1f} min")
    print(f"Output:      {OUTPUT_FILE} ({len(all_enriched)} records)")


if __name__ == "__main__":
    asyncio.run(main())
