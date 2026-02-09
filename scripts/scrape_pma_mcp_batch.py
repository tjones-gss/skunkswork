"""
PMA Profile Batch Scraper — generates batch JSON files for MCP Playwright processing.

This script prepares batches of PMA member IDs for scraping via MCP browser_run_code.
It also merges scraped results back into enriched JSONL.

Usage:
    python scripts/scrape_pma_mcp_batch.py prepare     # Create batch files
    python scripts/scrape_pma_mcp_batch.py merge        # Merge results into JSONL
    python scripts/scrape_pma_mcp_batch.py status       # Show progress
"""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_smoke_test.jsonl"
OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_enriched.jsonl"
BATCHES_DIR = PROJECT_ROOT / "data" / "raw" / "PMA" / "mcp_batches"
RESULTS_DIR = PROJECT_ROOT / "data" / "raw" / "PMA" / "mcp_results"

BATCH_SIZE = 25  # profiles per MCP call


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
    # Deduplicate by member_id
    seen = set()
    unique = []
    for r in records:
        mid = r.get("member_id", "")
        if mid and mid not in seen:
            seen.add(mid)
            unique.append(r)
    return unique


def load_already_scraped():
    """Load member_ids that have already been scraped from results dir."""
    scraped = {}
    if not RESULTS_DIR.exists():
        return scraped
    for f in sorted(RESULTS_DIR.glob("result_*.json")):
        with open(f, encoding="utf-8") as fh:
            data = json.load(fh)
            for item in data:
                mid = item.get("member_id", "")
                if mid:
                    scraped[mid] = item
    return scraped


def prepare_batches():
    records = load_records()
    already = load_already_scraped()
    to_scrape = [r for r in records if r.get("member_id") not in already]

    print(f"Total records: {len(records)}")
    print(f"Already scraped: {len(already)}")
    print(f"Remaining: {len(to_scrape)}")

    BATCHES_DIR.mkdir(parents=True, exist_ok=True)

    batches = []
    for i in range(0, len(to_scrape), BATCH_SIZE):
        batch = to_scrape[i:i + BATCH_SIZE]
        batch_ids = [{"member_id": r["member_id"], "profile_url": r["profile_url"]} for r in batch]
        batches.append(batch_ids)

    for i, batch in enumerate(batches):
        path = BATCHES_DIR / f"batch_{i:04d}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(batch, f)
        print(f"  Wrote {path.name}: {len(batch)} profiles")

    print(f"\nTotal batches: {len(batches)}")
    print(f"Use MCP browser_run_code to process each batch.")
    print(f"Results go in: {RESULTS_DIR}/result_NNNN.json")


def merge_results():
    records = load_records()
    records_by_id = {r["member_id"]: r for r in records}
    scraped = load_already_scraped()

    print(f"Records: {len(records)}, Scraped results: {len(scraped)}")

    enriched = []
    stats = {"websites": 0, "phones": 0, "contacts": 0, "emails": 0, "tiers": 0}

    for r in records:
        mid = r["member_id"]
        if mid in scraped:
            profile = scraped[mid]
            merged = dict(r)
            merged["enriched_at"] = profile.get("enriched_at", datetime.now(timezone.utc).isoformat())
            if profile.get("website"):
                merged["website"] = profile["website"]
                merged["domain"] = extract_domain(profile["website"])
                stats["websites"] += 1
            if profile.get("phone"):
                merged["phone"] = profile["phone"]
                stats["phones"] += 1
            if profile.get("fax"):
                merged["fax"] = profile["fax"]
            if profile.get("street"):
                merged["street"] = profile["street"]
            if profile.get("zip"):
                merged["zip_code"] = profile["zip"]
            if profile.get("city"):
                merged["city"] = profile["city"]
            if profile.get("state"):
                merged["state"] = profile["state"]
            if profile.get("country"):
                merged["country"] = profile["country"]
            if profile.get("employees"):
                merged["employees"] = profile["employees"]
                emp = profile["employees"]
                m = re.match(r"(\d[\d,]*)\s*-\s*(\d[\d,]*)", emp)
                if m:
                    merged["employee_count_min"] = int(m.group(1).replace(",", ""))
                    merged["employee_count_max"] = int(m.group(2).replace(",", ""))
                elif "+" in emp:
                    num = re.search(r"(\d[\d,]*)", emp)
                    if num:
                        merged["employee_count_min"] = int(num.group(1).replace(",", ""))
            if profile.get("facilitySize"):
                merged["facility_size_sqft"] = profile["facilitySize"]
            if profile.get("tier"):
                merged["membership_tier"] = profile["tier"]
                stats["tiers"] += 1
            if profile.get("memberSince"):
                merged["member_since"] = profile["memberSince"]
            if profile.get("processes"):
                merged["manufacturing_processes"] = profile["processes"]
            if profile.get("certifications"):
                merged["certifications"] = profile["certifications"]
            if profile.get("markets"):
                merged["markets_served"] = profile["markets"]
            if profile.get("description"):
                merged["description"] = profile["description"]
            if profile.get("contacts"):
                merged["contacts"] = profile["contacts"]
                stats["contacts"] += 1
                stats["emails"] += sum(1 for c in profile["contacts"] if c.get("email"))
            enriched.append(merged)
        else:
            enriched.append(r)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for r in enriched:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"\nMerge Complete:")
    print(f"  Total records: {len(enriched)}")
    print(f"  Enriched:      {len(scraped)}/{len(records)} ({len(scraped)/len(records)*100:.0f}%)")
    print(f"  Websites:      {stats['websites']} ({stats['websites']/max(len(scraped),1)*100:.0f}%)")
    print(f"  Phones:        {stats['phones']} ({stats['phones']/max(len(scraped),1)*100:.0f}%)")
    print(f"  Contacts:      {stats['contacts']} records")
    print(f"  Emails:        {stats['emails']} total")
    print(f"  Tiers:         {stats['tiers']}")
    print(f"  Output:        {OUTPUT_FILE}")


def show_status():
    records = load_records()
    scraped = load_already_scraped()
    remaining = len(records) - len(scraped)

    # Count batch files
    batch_count = len(list(BATCHES_DIR.glob("batch_*.json"))) if BATCHES_DIR.exists() else 0
    result_count = len(list(RESULTS_DIR.glob("result_*.json"))) if RESULTS_DIR.exists() else 0

    print(f"PMA Scraping Status:")
    print(f"  Total records:    {len(records)}")
    print(f"  Scraped:          {len(scraped)} ({len(scraped)/len(records)*100:.0f}%)")
    print(f"  Remaining:        {remaining}")
    print(f"  Batch files:      {batch_count}")
    print(f"  Result files:     {result_count}")

    # Show website coverage in scraped
    websites = sum(1 for v in scraped.values() if v.get("website"))
    phones = sum(1 for v in scraped.values() if v.get("phone"))
    contacts = sum(1 for v in scraped.values() if v.get("contacts"))
    print(f"  With website:     {websites} ({websites/max(len(scraped),1)*100:.0f}%)")
    print(f"  With phone:       {phones} ({phones/max(len(scraped),1)*100:.0f}%)")
    print(f"  With contacts:    {contacts} ({contacts/max(len(scraped),1)*100:.0f}%)")

    # Next batch to process
    if BATCHES_DIR.exists():
        processed = set()
        if RESULTS_DIR.exists():
            for f in RESULTS_DIR.glob("result_*.json"):
                processed.add(f.stem.replace("result_", "batch_"))
        pending = sorted([f.stem for f in BATCHES_DIR.glob("batch_*.json") if f.stem not in processed])
        if pending:
            print(f"\n  Next batch: {pending[0]}")
            print(f"  Pending batches: {len(pending)}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/scrape_pma_mcp_batch.py [prepare|merge|status]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "prepare":
        prepare_batches()
    elif cmd == "merge":
        merge_results()
    elif cmd == "status":
        show_status()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
