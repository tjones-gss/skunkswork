"""
PMA Profile Scraper via MCP Playwright.

This script generates a batch of profile URLs and scrapes them
using the already-established MCP Playwright browser session.

It works in chunks: navigate to page, extract data via JS, write JSONL.

Usage:
    python scripts/scrape_pma_mcp.py [--start N] [--count N]
"""

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_smoke_test.jsonl"
OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_enriched.jsonl"


def extract_domain(website_url: str) -> str:
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


def load_records() -> list[dict]:
    records = []
    with open(INPUT_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def load_already_scraped() -> set[str]:
    scraped = set()
    if OUTPUT_FILE.exists():
        with open(OUTPUT_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    rec = json.loads(line)
                    if rec.get("enriched_at") and rec.get("member_id"):
                        scraped.add(rec["member_id"])
    return scraped


def merge_profile_data(record: dict, profile: dict) -> dict:
    """Merge extracted profile data into the existing record."""
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
    if profile.get("city"):
        enriched["city"] = profile["city"]
    if profile.get("state"):
        enriched["state"] = profile["state"]
    if profile.get("zip"):
        enriched["zip_code"] = profile["zip"]
    if profile.get("country"):
        enriched["country"] = profile["country"]
    if profile.get("employees"):
        enriched["employees"] = profile["employees"]
        emp = profile["employees"]
        m = re.match(r"(\d[\d,]*)\s*-\s*(\d[\d,]*)", emp)
        if m:
            enriched["employee_count_min"] = int(m.group(1).replace(",", ""))
            enriched["employee_count_max"] = int(m.group(2).replace(",", ""))
        elif emp.endswith("+"):
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


# The JS extraction function to run on each profile page
EXTRACT_JS = """() => {
  const article = document.querySelector('article');
  if (!article) return null;

  const h2 = article.querySelectorAll('h2');
  const companyName = h2.length > 1 ? h2[1].textContent.trim() : '';

  const addressDiv = article.querySelector('.company-address');
  let street = '', city = '', state = '', zip = '', country = '';
  if (addressDiv) {
    const text = addressDiv.innerText;
    const lines = text.split('\\n').map(l => l.trim()).filter(l => l && l !== 'Map Location');
    if (lines[0]) street = lines[0];
    if (lines[1]) {
      const m = lines[1].match(/^(.+?),\\s+([A-Z]{2})\\s+(\\d{5}(?:-\\d{4})?)$/);
      if (m) { city = m[1]; state = m[2]; zip = m[3]; }
    }
    if (lines[2]) country = lines[2];
  }

  const contactDiv = article.querySelector('.company-contact');
  let phone = '', fax = '', website = '';
  if (contactDiv) {
    const text = contactDiv.innerText;
    const pm = text.match(/Phone:\\s*([^\\n]+)/);
    if (pm) phone = pm[1].trim();
    const fm = text.match(/Fax:\\s*([^\\n]+)/);
    if (fm) fax = fm[1].trim();
    const link = contactDiv.querySelector('a[href]');
    if (link) {
      const href = link.getAttribute('href');
      if (href.includes('/go.asp?url=')) {
        website = href.split('/go.asp?url=')[1];
      } else if (href.startsWith('http')) {
        website = href;
      } else {
        const lt = link.textContent.trim();
        if (lt && lt.includes('.')) website = 'http://' + lt;
      }
    }
  }

  const facilityDiv = article.querySelector('.company-facility');
  let employees = '', facilitySize = '';
  if (facilityDiv) {
    const text = facilityDiv.innerText;
    const em = text.match(/Employees:\\s*([^\\n]+)/);
    if (em) employees = em[1].trim();
    const sm = text.match(/Facility Size:\\s*([^\\n]+)/);
    if (sm) facilitySize = sm[1].trim();
  }

  const levelDiv = article.querySelector('.company-level');
  let tier = '', memberSince = '';
  if (levelDiv) {
    const text = levelDiv.innerText;
    const tm = text.match(/(PLATINUM|GOLD|SILVER|BRONZE|PREMIER)\\s+MEMBER/i);
    if (tm) tier = tm[1].toUpperCase();
    const sm2 = text.match(/Member Since:\\s*(\\d{4})/);
    if (sm2) memberSince = sm2[1];
  }

  function getListSection(heading) {
    const items = [];
    article.querySelectorAll('h3').forEach(h3 => {
      if (h3.textContent.includes(heading)) {
        const parent = h3.closest('.member-search-blocks');
        if (parent) {
          parent.querySelectorAll('div[style]').forEach(d => {
            const t = d.textContent.trim().replace(/^[\\s\\u00a0]+/, '');
            if (t) items.push(t);
          });
        }
      }
    });
    return items;
  }

  const processes = getListSection('Manufacturing Processes');
  const certifications = getListSection('Certifications');
  const markets = getListSection('Markets Served');

  let description = '';
  article.querySelectorAll('h3').forEach(h3 => {
    if (h3.textContent.includes('Company Description')) {
      const parent = h3.closest('.member-search-blocks');
      if (parent) {
        description = parent.textContent.replace('Company Description', '').trim();
      }
    }
  });

  // Extract contacts from HTML comments
  const contacts = [];
  const html = document.documentElement.innerHTML;
  const commentPattern = /<!--([\\s\\S]*?)-->/g;
  let cm;
  while ((cm = commentPattern.exec(html)) !== null) {
    if (!cm[1].includes('contact-info')) continue;
    const blocks = cm[1].split('<div class="contact-info"');
    for (let i = 1; i < blocks.length; i++) {
      const block = blocks[i];
      const contact = {};
      const nm = block.match(/<strong>(.*?)<\\/strong>/);
      if (nm) contact.name = nm[1].trim();
      const titleM = block.match(/<\\/strong>\\s*<br>\\s*\\n?\\s*(.+?)\\s*<br>/);
      if (titleM) {
        const t = titleM[1].trim();
        if (!t.startsWith('Phone') && !t.startsWith('Fax') && !t.startsWith('Email') && !t.startsWith('<')) {
          contact.title = t;
        }
      }
      const phM = block.match(/Phone:\\s*(?:&nbsp;)?\\s*([^<\\n]+)/);
      if (phM) contact.phone = phM[1].trim();
      const emM = block.match(/<a\\s+class="no-mail"\\s+name="([^"]+)"\\s+rel="([^"]+)"/);
      if (emM) {
        contact.email = emM[1] + '@' + emM[2].replace('#', '.');
      }
      if (contact.name) contacts.push(contact);
    }
  }

  return {
    companyName, street, city, state, zip, country,
    phone, fax, website, employees, facilitySize,
    tier, memberSince, processes, certifications, markets,
    description, contacts
  };
}"""


def print_progress(records: list[dict]):
    """Print summary stats for enriched records."""
    total = len(records)
    websites = sum(1 for r in records if r.get("website"))
    phones = sum(1 for r in records if r.get("phone"))
    employees = sum(1 for r in records if r.get("employees") or r.get("employee_count_min"))
    contacts = sum(1 for r in records if r.get("contacts"))
    emails = sum(len(r.get("contacts", [])) for r in records if r.get("contacts"))
    processes = sum(1 for r in records if r.get("manufacturing_processes"))
    tiers = sum(1 for r in records if r.get("membership_tier"))

    print(f"\n{'='*60}")
    print(f"PMA Profile Scraping Results ({total} records)")
    print(f"{'='*60}")
    print(f"  Websites:   {websites:>5} ({websites/total*100:.0f}%)")
    print(f"  Phones:     {phones:>5} ({phones/total*100:.0f}%)")
    print(f"  Employees:  {employees:>5} ({employees/total*100:.0f}%)")
    print(f"  Contacts:   {contacts:>5} records with contacts")
    print(f"  Emails:     {emails:>5} total email addresses")
    print(f"  Processes:  {processes:>5}")
    print(f"  Tier:       {tiers:>5}")


if __name__ == "__main__":
    # This script is meant to be run manually with instructions
    # The actual scraping happens via MCP Playwright calls
    records = load_records()
    already = load_already_scraped()
    print(f"Total records: {len(records)}")
    print(f"Already scraped: {len(already)}")
    print(f"Remaining: {len(records) - len(already)}")
    print(f"\nProfile URLs for scraping:")
    for i, r in enumerate(records[:5]):
        print(f"  {i+1}. {r['profile_url']} ({r['company_name']})")
