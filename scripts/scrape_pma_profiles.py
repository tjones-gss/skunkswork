"""
Scrape PMA member profile pages to extract website, phone, address, contacts,
and other firmographic data for 1,064 companies.

Uses Playwright (headed mode) to bypass Cloudflare protection.

Usage:
    python scripts/scrape_pma_profiles.py [--limit N] [--resume]
"""

import argparse
import asyncio
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_smoke_test.jsonl"
OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_enriched.jsonl"

DELAY_SECONDS = 2.0  # 0.5 req/sec

# Stealth script to evade bot detection
_STEALTH_SCRIPT = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en']});
window.chrome = {runtime: {}};
"""


def extract_domain(website_url: str) -> str:
    """Extract domain from a website URL."""
    if not website_url:
        return ""
    url = website_url.strip()
    if not url.startswith(("http://", "https://")):
        url = "http://" + url
    try:
        parsed = urlparse(url)
        domain = parsed.netloc or parsed.path.split("/")[0]
        domain = domain.lower().removeprefix("www.")
        return domain
    except Exception:
        return ""


def parse_address(address_div) -> dict:
    """Parse the company-address div into structured fields."""
    result = {"street": "", "city": "", "state": "", "zip_code": "", "country": ""}
    if not address_div:
        return result

    for br in address_div.find_all("br"):
        br.replace_with("\n")
    text = address_div.get_text(separator="\n", strip=True)
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    lines = [l for l in lines if l != "Map Location" and not l.startswith("\xa0")]

    if len(lines) >= 1:
        result["street"] = lines[0]
    if len(lines) >= 2:
        city_state_zip = lines[1].replace("\xa0", " ")
        m = re.match(r"^(.+?),\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$", city_state_zip)
        if m:
            result["city"] = m.group(1).strip()
            result["state"] = m.group(2)
            result["zip_code"] = m.group(3)
        else:
            parts = city_state_zip.split(",")
            if len(parts) >= 2:
                result["city"] = parts[0].strip()
                rest = parts[1].strip()
                state_zip = rest.split()
                if state_zip:
                    result["state"] = state_zip[0]
                if len(state_zip) > 1:
                    result["zip_code"] = state_zip[1]
    if len(lines) >= 3:
        result["country"] = lines[2]

    return result


def parse_phone_fax_website(contact_div) -> dict:
    """Parse the company-contact div for phone, fax, website."""
    result = {"phone": "", "fax": "", "website": ""}
    if not contact_div:
        return result

    text = contact_div.get_text(separator="\n", strip=True)

    phone_match = re.search(r"Phone:\s*([^\n]+)", text)
    if phone_match:
        result["phone"] = phone_match.group(1).strip()

    fax_match = re.search(r"Fax:\s*([^\n]+)", text)
    if fax_match:
        result["fax"] = fax_match.group(1).strip()

    website_link = contact_div.find("a", href=True)
    if website_link:
        href = website_link["href"]
        if "/go.asp?url=" in href:
            result["website"] = href.split("/go.asp?url=", 1)[1]
        elif href.startswith("http"):
            result["website"] = href
        else:
            link_text = website_link.get_text(strip=True)
            if link_text and "." in link_text:
                result["website"] = "http://" + link_text

    return result


def parse_facility_info(facility_div) -> dict:
    """Parse facility size and employee count from company-facility div."""
    result = {"facility_size_sqft": "", "employees": ""}
    if not facility_div:
        return result

    text = facility_div.get_text(separator="\n", strip=True)

    size_match = re.search(r"Facility Size:\s*([^\n]+)", text)
    if size_match:
        size_str = size_match.group(1).strip()
        num_match = re.search(r"([\d,]+)", size_str)
        if num_match:
            result["facility_size_sqft"] = num_match.group(1).replace(",", "")

    emp_match = re.search(r"Employees:\s*([^\n]+)", text)
    if emp_match:
        result["employees"] = emp_match.group(1).strip()

    return result


def parse_membership(level_div) -> dict:
    """Parse membership tier and year from company-level div."""
    result = {"membership_tier": "", "member_since": ""}
    if not level_div:
        return result

    text = level_div.get_text(separator="\n", strip=True)

    tier_match = re.search(r"(PLATINUM|GOLD|SILVER|BRONZE|PREMIER)\s+MEMBER", text, re.IGNORECASE)
    if tier_match:
        result["membership_tier"] = tier_match.group(1).upper()

    since_match = re.search(r"Member Since:\s*(\d{4})", text)
    if since_match:
        result["member_since"] = since_match.group(1)

    return result


def parse_contacts_from_comments(html_text: str) -> list:
    """Extract contact information from HTML comments.

    PMA hides contacts in commented-out HTML like:
    <!--<div class="member-search-blocks">
        <div class="contact-info">
            <strong>Mr. Dan Bridges </strong><br>
            President<br>
            Phone: (714) 995-8313<br>
            Email: <a class="no-mail" name="danbridges" rel="aggrengr#com">Send Email</a>
        </div>
    </div>-->
    """
    contacts = []

    comment_pattern = re.compile(r"<!--(.*?)-->", re.DOTALL)
    for match in comment_pattern.finditer(html_text):
        comment_text = match.group(1)
        if "contact-info" not in comment_text:
            continue

        contact_blocks = re.split(r'<div class="contact-info"', comment_text)
        for block in contact_blocks[1:]:
            contact = {"name": "", "title": "", "phone": "", "fax": "", "email": ""}

            name_match = re.search(r"<strong>(.*?)</strong>", block)
            if name_match:
                contact["name"] = name_match.group(1).strip()

            title_match = re.search(r"</strong>\s*<br>\s*\n?\s*(.+?)\s*<br>", block)
            if title_match:
                title = title_match.group(1).strip()
                if not title.startswith(("Phone", "Fax", "Email", "<")):
                    contact["title"] = title

            phone_match = re.search(r"Phone:\s*&nbsp;?\s*([^<\n]+)", block)
            if phone_match:
                contact["phone"] = phone_match.group(1).strip()

            fax_match = re.search(r"Fax:\s*([^<\n]+)", block)
            if fax_match:
                contact["fax"] = fax_match.group(1).strip()

            email_match = re.search(r'<a\s+class="no-mail"\s+name="([^"]+)"\s+rel="([^"]+)"', block)
            if email_match:
                username = email_match.group(1)
                domain_encoded = email_match.group(2)
                domain = domain_encoded.replace("#", ".")
                contact["email"] = f"{username}@{domain}"

            if contact["name"]:
                contacts.append(contact)

    return contacts


def parse_list_section(article, heading_text: str) -> list:
    """Parse a section like Manufacturing Processes, Certifications, etc."""
    items = []
    for h3 in article.find_all("h3"):
        if heading_text.lower() in h3.get_text(strip=True).lower():
            parent_div = h3.find_parent("div", class_="member-search-blocks")
            if parent_div:
                for item_div in parent_div.find_all("div", style=True):
                    text = item_div.get_text(strip=True)
                    if text:
                        text = re.sub(r"^[\s\u00a0]+", "", text)
                        items.append(text)
            break
    return items


def parse_description(article) -> str:
    """Parse the company description."""
    for h3 in article.find_all("h3"):
        if "Company Description" in h3.get_text(strip=True):
            parent_div = h3.find_parent("div", class_="member-search-blocks")
            if parent_div:
                full_text = parent_div.get_text(strip=True)
                desc = full_text.replace("Company Description", "", 1).strip()
                return desc
    return ""


def parse_profile_page(html: str, record: dict) -> dict:
    """Parse a PMA profile page and merge data into the existing record."""
    soup = BeautifulSoup(html, "html.parser")
    article = soup.find("article")
    if not article:
        return record

    enriched = dict(record)
    enriched["enriched_at"] = datetime.now(timezone.utc).isoformat()

    # Address
    address_div = article.find("div", class_="company-address")
    address_data = parse_address(address_div)
    if address_data["street"]:
        enriched["street"] = address_data["street"]
    if address_data["zip_code"]:
        enriched["zip_code"] = address_data["zip_code"]
    if address_data["city"]:
        enriched["city"] = address_data["city"]
    if address_data["state"]:
        enriched["state"] = address_data["state"]
    if address_data["country"]:
        enriched["country"] = address_data["country"]

    # Phone, Fax, Website
    contact_div = article.find("div", class_="company-contact")
    contact_data = parse_phone_fax_website(contact_div)
    if contact_data["phone"]:
        enriched["phone"] = contact_data["phone"]
    if contact_data["fax"]:
        enriched["fax"] = contact_data["fax"]
    if contact_data["website"]:
        enriched["website"] = contact_data["website"]
        enriched["domain"] = extract_domain(contact_data["website"])

    # Facility info
    facility_div = article.find("div", class_="company-facility")
    facility_data = parse_facility_info(facility_div)
    if facility_data["facility_size_sqft"]:
        enriched["facility_size_sqft"] = facility_data["facility_size_sqft"]
    if facility_data["employees"]:
        enriched["employees"] = facility_data["employees"]
        emp_range = facility_data["employees"]
        emp_match = re.match(r"(\d[\d,]*)\s*-\s*(\d[\d,]*)", emp_range)
        if emp_match:
            enriched["employee_count_min"] = int(emp_match.group(1).replace(",", ""))
            enriched["employee_count_max"] = int(emp_match.group(2).replace(",", ""))
        elif emp_range.endswith("+"):
            num = re.search(r"(\d[\d,]*)", emp_range)
            if num:
                enriched["employee_count_min"] = int(num.group(1).replace(",", ""))

    # Membership
    level_div = article.find("div", class_="company-level")
    membership_data = parse_membership(level_div)
    if membership_data["membership_tier"]:
        enriched["membership_tier"] = membership_data["membership_tier"]
    if membership_data["member_since"]:
        enriched["member_since"] = membership_data["member_since"]

    # Contacts from HTML comments
    contacts = parse_contacts_from_comments(html)
    if contacts:
        enriched["contacts"] = contacts

    # Company description
    description = parse_description(article)
    if description:
        enriched["description"] = description

    # Manufacturing processes
    processes = parse_list_section(article, "Manufacturing Processes")
    if processes:
        enriched["manufacturing_processes"] = processes

    # Certifications
    certs = parse_list_section(article, "Certifications")
    if certs:
        enriched["certifications"] = certs

    # Markets served
    markets = parse_list_section(article, "Markets Served")
    if markets:
        enriched["markets_served"] = markets

    # Electronic capabilities
    ecap = parse_list_section(article, "Electronic Capabilities")
    if ecap:
        enriched["electronic_capabilities"] = ecap

    return enriched


def load_records(filepath: Path) -> list:
    """Load JSONL records from file."""
    records = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_already_scraped(filepath: Path) -> set:
    """Load member_ids that have already been scraped (for resume support)."""
    scraped = set()
    if filepath.exists():
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    if rec.get("enriched_at") and rec.get("member_id"):
                        scraped.add(rec["member_id"])
    return scraped


async def scrape_profiles(args):
    """Main scraping function using Playwright."""

    if not INPUT_FILE.exists():
        print(f"ERROR: Input file not found: {INPUT_FILE}")
        sys.exit(1)

    # Load input records
    records = load_records(INPUT_FILE)
    print(f"Loaded {len(records)} records from {INPUT_FILE.name}")

    # Deduplicate by member_id
    seen_ids = set()
    unique_records = []
    for r in records:
        mid = r.get("member_id", "")
        if mid and mid not in seen_ids:
            seen_ids.add(mid)
            unique_records.append(r)
        elif not mid:
            unique_records.append(r)
    print(f"Unique records by member_id: {len(unique_records)}")

    # Resume support
    already_scraped = set()
    if args.resume and OUTPUT_FILE.exists():
        already_scraped = load_already_scraped(OUTPUT_FILE)
        print(f"Resuming: {len(already_scraped)} already scraped, skipping them")

    # Filter out already-scraped records
    to_scrape = [r for r in unique_records if r.get("member_id") not in already_scraped]
    if args.limit > 0:
        to_scrape = to_scrape[:args.limit]
    print(f"Will scrape {len(to_scrape)} profiles")

    if not to_scrape:
        print("Nothing to scrape!")
        return

    # Ensure output directory exists
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    # Stats
    stats = {
        "total": len(to_scrape),
        "scraped": 0,
        "websites_found": 0,
        "phones_found": 0,
        "contacts_found": 0,
        "emails_found": 0,
        "descriptions_found": 0,
        "errors": 0,
        "skipped_no_url": 0,
    }

    mode = "a" if args.resume else "w"

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        await context.add_init_script(_STEALTH_SCRIPT)
        page = await context.new_page()

        # Visit the directory page first (establish session/cookies)
        # Must wait for Cloudflare JS challenge to complete
        print("Visiting PMA directory to establish session (waiting for Cloudflare)...")
        await page.goto("https://www.pma.org/directory/", wait_until="domcontentloaded", timeout=60000)
        # Wait for Cloudflare challenge to resolve (typically 5-8 seconds)
        print("Waiting 10s for Cloudflare JS challenge...")
        await asyncio.sleep(10)

        # Visit first profile to fully establish session
        first_url = to_scrape[0].get("profile_url", "")
        if first_url:
            print(f"Visiting first profile to confirm access...")
            await page.goto(first_url, wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(5)
            try:
                await page.wait_for_selector("article", timeout=15000)
                print("Session established successfully!")
            except Exception:
                print("WARNING: Cloudflare challenge may still be active. Will retry per-page...")

        with open(OUTPUT_FILE, mode, encoding="utf-8") as out_f:
            start_time = time.time()

            for i, record in enumerate(to_scrape):
                profile_url = record.get("profile_url", "")
                company = record.get("company_name", "Unknown")

                if not profile_url:
                    stats["skipped_no_url"] += 1
                    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
                    continue

                pct = ((i + 1) / len(to_scrape)) * 100
                elapsed = time.time() - start_time
                rate = (i + 1) / max(elapsed, 1) * 3600
                eta_hrs = (len(to_scrape) - i - 1) / max(rate, 1)
                print(f"[{i+1}/{len(to_scrape)} {pct:.0f}% ETA:{eta_hrs:.1f}h] {company} ...",
                      end=" ", flush=True)

                try:
                    resp = await page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
                    status = resp.status if resp else 0

                    # If we got a Cloudflare challenge page (403), wait and retry
                    if status == 403:
                        print("CF...", end=" ", flush=True)
                        await asyncio.sleep(8)
                        # Check if the page resolved after the challenge
                        try:
                            await page.wait_for_selector("article", timeout=10000)
                            status = 200  # Challenge passed
                        except Exception:
                            # Try reloading
                            resp = await page.reload(wait_until="domcontentloaded", timeout=30000)
                            status = resp.status if resp else 0
                            if status == 403:
                                await asyncio.sleep(5)
                                try:
                                    await page.wait_for_selector("article", timeout=10000)
                                    status = 200
                                except Exception:
                                    pass

                    if status == 200:
                        # Wait for article to render
                        await page.wait_for_selector("article", timeout=5000)
                        html = await page.content()
                        enriched = parse_profile_page(html, record)
                        stats["scraped"] += 1

                        if enriched.get("website"):
                            stats["websites_found"] += 1
                        if enriched.get("phone"):
                            stats["phones_found"] += 1
                        if enriched.get("contacts"):
                            stats["contacts_found"] += 1
                            stats["emails_found"] += sum(
                                1 for c in enriched["contacts"] if c.get("email")
                            )
                        if enriched.get("description"):
                            stats["descriptions_found"] += 1

                        out_f.write(json.dumps(enriched, ensure_ascii=False) + "\n")
                        print(f"OK (web={'Y' if enriched.get('website') else 'N'} "
                              f"ph={'Y' if enriched.get('phone') else 'N'} "
                              f"contacts={len(enriched.get('contacts', []))})")
                    else:
                        print(f"HTTP {status}")
                        stats["errors"] += 1
                        out_f.write(json.dumps(record, ensure_ascii=False) + "\n")

                except Exception as e:
                    err_msg = str(e)[:80]
                    print(f"ERROR: {err_msg}")
                    stats["errors"] += 1
                    out_f.write(json.dumps(record, ensure_ascii=False) + "\n")

                # Flush periodically
                if (i + 1) % 10 == 0:
                    out_f.flush()

                # Rate limit
                if i < len(to_scrape) - 1:
                    await asyncio.sleep(DELAY_SECONDS)

        await browser.close()

    # Print summary
    total_elapsed = time.time() - start_time
    print("\n" + "=" * 60)
    print("PMA Profile Scraping Summary")
    print("=" * 60)
    print(f"Total profiles:      {stats['total']}")
    print(f"Successfully scraped: {stats['scraped']}")
    print(f"Websites found:      {stats['websites_found']} "
          f"({stats['websites_found']/max(stats['scraped'],1)*100:.0f}%)")
    print(f"Phones found:        {stats['phones_found']} "
          f"({stats['phones_found']/max(stats['scraped'],1)*100:.0f}%)")
    print(f"Contacts found:      {stats['contacts_found']} records with contacts")
    print(f"Emails found:        {stats['emails_found']} total email addresses")
    print(f"Descriptions found:  {stats['descriptions_found']}")
    print(f"Errors:              {stats['errors']}")
    print(f"Skipped (no URL):    {stats['skipped_no_url']}")
    print(f"Elapsed:             {total_elapsed/60:.1f} min")
    print(f"Output: {OUTPUT_FILE}")
    if args.resume and already_scraped:
        print(f"Previously scraped:  {len(already_scraped)}")
        print(f"Total in output:     {len(already_scraped) + stats['scraped']}")


def main():
    parser = argparse.ArgumentParser(description="Scrape PMA profile pages")
    parser.add_argument("--limit", type=int, default=0,
                        help="Limit number of profiles to scrape (0=all)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from where we left off")
    args = parser.parse_args()
    asyncio.run(scrape_profiles(args))


if __name__ == "__main__":
    main()
