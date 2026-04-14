#!/usr/bin/env python3
"""Extract NADCA Corporate Members directory — all on one static page."""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT = BASE_DIR / "data" / "raw" / "NADCA" / "records.jsonl"
URL = "https://www.diecasting.org/Web/Resources/Directories/Corporate_Members/Web/Resources/Directories/Corporate_Members.aspx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


def extract():
    print(f"Fetching NADCA Corporate Members directory...")
    resp = httpx.get(URL, headers=HEADERS, timeout=30, follow_redirects=True)
    resp.raise_for_status()
    print(f"  Status: {resp.status_code}, Size: {len(resp.text):,} chars")

    soup = BeautifulSoup(resp.text, "html.parser")
    now = datetime.now(timezone.utc).isoformat()

    records = []
    seen = set()

    # NADCA uses ASP.NET — look for links in the directory listing
    # The page has company names as links within the directory content
    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        text = link.get_text(strip=True)

        # Skip navigation, empty, internal links
        if not text or len(text) < 3:
            continue
        if href.startswith("#") or href.startswith("javascript:"):
            continue
        if "diecasting.org" in href and "directories" not in href.lower():
            continue

        # Check if this looks like a company entry (has external website link)
        if href.startswith("http") and "diecasting.org" not in href:
            name = text.strip()
            website = href.strip()

            # Normalize for dedup
            key = re.sub(r"[^a-z0-9]", "", name.lower())
            if key in seen or len(key) < 3:
                continue
            seen.add(key)

            # Extract domain from website
            domain = ""
            try:
                from urllib.parse import urlparse
                parsed = urlparse(website)
                domain = parsed.netloc.replace("www.", "")
            except Exception:
                pass

            records.append({
                "company_name": name,
                "website": website,
                "domain": domain,
                "association": "NADCA",
                "source_url": URL,
                "extracted_at": now,
                "country": "United States",
            })

    # Also look for company names in table rows or list items without links
    for td in soup.find_all(["td", "li", "div"]):
        text = td.get_text(strip=True)
        # Look for patterns like "Company Name" followed by a link
        inner_link = td.find("a", href=True)
        if inner_link:
            href = inner_link.get("href", "")
            name_text = text.replace(inner_link.get_text(strip=True), "").strip()
            if not name_text:
                name_text = inner_link.get_text(strip=True)

    print(f"  Extracted {len(records)} companies with websites")

    # Write output
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"  Saved to: {OUTPUT}")
    return records


if __name__ == "__main__":
    records = extract()
    print(f"\nTotal: {len(records)} NADCA companies extracted")
