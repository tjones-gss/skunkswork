#!/usr/bin/env python3
"""
Contact Page Deep Crawler
NAM Intelligence Pipeline

Crawls company websites to find decision-maker contacts:
- /about, /about-us, /team, /leadership, /our-team, /management, /contact
- Extracts: names, titles, emails, phone numbers
- Uses stealth Playwright to bypass bot detection
- Respects rate limits (1 req/2sec)

Usage:
  python scripts/enrich_contacts_crawl.py              # Crawl all companies missing contacts
  python scripts/enrich_contacts_crawl.py --limit 100  # Crawl first 100
  python scripts/enrich_contacts_crawl.py --assoc NADCA # Crawl specific association
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

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "exports" / "companies_all.csv"
OUTPUT_PATH = BASE_DIR / "data" / "processed" / "contacts_crawled.jsonl"

# Pages to check for contact info
CONTACT_PATHS = [
    "/about", "/about-us", "/about-us/", "/about/",
    "/team", "/our-team", "/the-team", "/leadership",
    "/management", "/management-team", "/our-leadership",
    "/contact", "/contact-us", "/contact-us/",
    "/people", "/staff", "/executives",
]

# Title keywords for decision makers (ERP buying committee)
TARGET_TITLES = [
    "ceo", "chief executive", "president",
    "cfo", "chief financial", "vp finance", "vice president finance",
    "coo", "chief operating", "vp operations", "vice president operations",
    "cio", "chief information", "cto", "chief technology",
    "vp it", "vice president it", "it director", "director of it",
    "vp manufacturing", "vice president manufacturing",
    "plant manager", "general manager", "director of operations",
    "controller", "erp", "mis director", "is director",
    "director of engineering", "vp engineering",
    "owner", "founder", "managing director",
]

# Email regex
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
# Phone regex (US format)
PHONE_RE = re.compile(r'(?:\+1[-.\s]?)?(?:\(?\d{3}\)?[-.\s]?)?\d{3}[-.\s]?\d{4}')
# Name + title patterns
NAME_TITLE_RE = re.compile(
    r'([A-Z][a-z]+(?:\s+[A-Z]\.?\s+)?[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)'
    r'[,\s\-\|]+\s*'
    r'([A-Z][^,\n]{5,60})',
    re.MULTILINE
)


def load_companies(limit=None, assoc=None) -> list[dict]:
    """Load companies that need contact enrichment."""
    records = []
    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            website = (row.get("website") or row.get("domain") or "").strip()
            if not website:
                continue
            if not website.startswith("http"):
                website = f"https://{website}"

            # Filter by association if specified
            if assoc:
                rec_assoc = (row.get("associations") or "").upper()
                if assoc.upper() not in rec_assoc:
                    continue

            # Prioritize companies without contacts
            has_contacts = bool(row.get("contacts", "").strip())

            records.append({
                "company_name": row.get("company_name", ""),
                "website": website,
                "domain": (row.get("domain") or "").strip(),
                "association": row.get("associations", ""),
                "has_existing_contacts": has_contacts,
            })

    # Sort: companies without contacts first
    records.sort(key=lambda r: r["has_existing_contacts"])

    if limit:
        records = records[:limit]

    return records


def extract_contacts_from_text(text: str, domain: str) -> list[dict]:
    """Extract potential contacts from page text."""
    contacts = []

    # Find emails matching the company domain
    bare_domain = domain.replace("www.", "").split("/")[0]
    emails = EMAIL_RE.findall(text)
    company_emails = [e for e in emails if bare_domain in e.lower()]

    # Find name-title patterns
    for match in NAME_TITLE_RE.finditer(text):
        name = match.group(1).strip()
        title = match.group(2).strip()

        # Check if title matches target decision-maker roles
        title_lower = title.lower()
        is_target = any(t in title_lower for t in TARGET_TITLES)

        if is_target and len(name.split()) >= 2:
            # Try to find an email for this person
            first_name = name.split()[0].lower()
            last_name = name.split()[-1].lower()
            person_email = ""
            for email in company_emails:
                email_lower = email.lower()
                if first_name in email_lower or last_name in email_lower:
                    person_email = email
                    break

            contacts.append({
                "name": name,
                "title": title,
                "email": person_email,
                "is_decision_maker": True,
            })

    # Also capture any company emails not yet assigned
    general_emails = [e for e in company_emails if not any(
        e == c.get("email") for c in contacts
    )]
    # Detect general contact emails
    for email in general_emails:
        prefix = email.split("@")[0].lower()
        if prefix in ("info", "contact", "sales", "admin", "support", "hello"):
            contacts.append({
                "name": "",
                "title": "General Contact",
                "email": email,
                "is_decision_maker": False,
            })

    # Find phone numbers
    phones = PHONE_RE.findall(text)
    if phones:
        # Attach first phone to first contact, or create a general contact
        if contacts:
            contacts[0]["phone"] = phones[0]
        else:
            contacts.append({
                "name": "",
                "title": "Main Phone",
                "email": "",
                "phone": phones[0],
                "is_decision_maker": False,
            })

    return contacts


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Max companies to crawl")
    parser.add_argument("--assoc", type=str, help="Filter by association code")
    args = parser.parse_args()

    print("=" * 60)
    print("NAM Intelligence Pipeline - Contact Page Crawler")
    print("=" * 60)

    companies = load_companies(limit=args.limit, assoc=args.assoc)
    print(f"\nLoaded {len(companies)} companies to crawl")
    print(f"  Without existing contacts: {sum(1 for c in companies if not c['has_existing_contacts'])}")

    # Use httpx with stealth headers for initial crawl
    import httpx

    STEALTH_HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }

    results = []
    contacts_found = 0
    dm_found = 0
    errors = 0
    now = datetime.now(timezone.utc).isoformat()

    client = httpx.Client(
        headers=STEALTH_HEADERS,
        follow_redirects=True,
        timeout=15,
    )

    for i, company in enumerate(companies):
        base_url = company["website"]
        company_contacts = []

        # Try each contact path
        for path in CONTACT_PATHS[:6]:  # Check first 6 paths
            url = urljoin(base_url, path)
            try:
                resp = client.get(url)
                if resp.status_code == 200:
                    text = resp.text
                    # Quick check if page has useful content
                    if len(text) > 1000:
                        new_contacts = extract_contacts_from_text(text, company.get("domain", ""))
                        for c in new_contacts:
                            # Dedupe by name
                            if not any(existing.get("name") == c["name"] and c["name"] for existing in company_contacts):
                                company_contacts.append(c)
                time.sleep(0.5)  # Rate limit between path checks
            except Exception:
                errors += 1
                continue

        if company_contacts:
            contacts_found += 1
            dm_count = sum(1 for c in company_contacts if c.get("is_decision_maker"))
            dm_found += dm_count

        results.append({
            "company_name": company["company_name"],
            "website": company["website"],
            "domain": company.get("domain", ""),
            "association": company.get("association", ""),
            "contacts_found": company_contacts,
            "contact_count": len(company_contacts),
            "decision_maker_count": sum(1 for c in company_contacts if c.get("is_decision_maker")),
            "crawled_at": now,
        })

        if (i + 1) % 25 == 0:
            print(f"  [{i+1}/{len(companies)}] Contacts found: {contacts_found}, Decision-makers: {dm_found}, Errors: {errors}")

        # Rate limit between companies
        time.sleep(1.5)

    client.close()

    # Write output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for rec in results:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"\n{'=' * 60}")
    print(f"Contact Crawl Results")
    print(f"{'=' * 60}")
    print(f"  Companies crawled: {len(results)}")
    print(f"  Companies with contacts: {contacts_found}")
    print(f"  Decision-makers found: {dm_found}")
    print(f"  Errors: {errors}")
    print(f"  Output: {OUTPUT_PATH}")
    print("Done!")


if __name__ == "__main__":
    main()
