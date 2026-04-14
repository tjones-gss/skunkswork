#!/usr/bin/env python3
"""Extract supplemental AGMA members from /resources/member-list/ page."""

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import httpx
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT = BASE_DIR / "data" / "raw" / "AGMA" / "records_supplemental.jsonl"
EXISTING = BASE_DIR / "data" / "raw" / "AGMA" / "records_live.jsonl"
URL = "https://agma.org/resources/member-list/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def load_existing_names() -> set:
    """Load normalized company names from existing AGMA records."""
    names = set()
    for path in [EXISTING, BASE_DIR / "data" / "raw" / "AGMA" / "records.jsonl"]:
        if path.exists():
            with open(path, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        try:
                            rec = json.loads(line)
                            key = re.sub(r"[^a-z0-9]", "", rec.get("company_name", "").lower())
                            if key:
                                names.add(key)
                        except json.JSONDecodeError:
                            continue
    return names


def extract():
    print(f"Fetching AGMA member list...")
    resp = httpx.get(URL, headers=HEADERS, timeout=30, follow_redirects=True)
    resp.raise_for_status()
    print(f"  Status: {resp.status_code}, Size: {len(resp.text):,} chars")

    soup = BeautifulSoup(resp.text, "html.parser")
    now = datetime.now(timezone.utc).isoformat()

    existing = load_existing_names()
    print(f"  Existing AGMA records: {len(existing)}")

    records = []
    new_count = 0
    seen = set()

    # AGMA member list page has links to company websites with company names
    for link in soup.find_all("a", href=True):
        href = link.get("href", "").strip()
        text = link.get_text(strip=True)

        if not text or len(text) < 3:
            continue
        if not href.startswith("http"):
            continue
        if "agma.org" in href:
            continue

        name = text
        key = re.sub(r"[^a-z0-9]", "", name.lower())
        if key in seen or len(key) < 3:
            continue
        seen.add(key)

        # Determine membership tier from parent elements
        tier = ""
        parent = link.find_parent(["div", "section", "li"])
        if parent:
            parent_text = parent.get_text(strip=True).lower()
            if "corporate" in parent_text:
                tier = "Corporate"
            elif "consultant" in parent_text:
                tier = "Consultant"
            elif "academic" in parent_text:
                tier = "Academic"
            elif "emeritus" in parent_text:
                tier = "Emeritus"

        # Extract domain
        domain = ""
        try:
            from urllib.parse import urlparse
            parsed = urlparse(href)
            domain = parsed.netloc.replace("www.", "")
        except Exception:
            pass

        is_new = key not in existing
        if is_new:
            new_count += 1

        records.append({
            "company_name": name,
            "website": href,
            "domain": domain,
            "association": "AGMA",
            "membership_tier": tier,
            "source_url": URL,
            "extracted_at": now,
            "country": "United States",
            "is_new": is_new,
        })

    print(f"  Total parsed: {len(records)}")
    print(f"  New (not in existing): {new_count}")

    # Write ALL records (including existing) to supplemental file
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT, "w", encoding="utf-8") as f:
        for rec in records:
            # Remove is_new flag before writing
            out = {k: v for k, v in rec.items() if k != "is_new"}
            f.write(json.dumps(out, ensure_ascii=False) + "\n")

    print(f"  Saved to: {OUTPUT}")
    return records, new_count


if __name__ == "__main__":
    records, new_count = extract()
    print(f"\nTotal: {len(records)} AGMA companies, {new_count} new")
