"""Merge browser-scraped PMA profile data with original records.

Usage:
    python scripts/merge_pma_batch.py <batch_json_file>

Reads batch results from a JSON file (output from browser scraping),
merges with original records from records_smoke_test.jsonl,
and appends to records_enriched.jsonl.
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_smoke_test.jsonl"
OUTPUT_FILE = PROJECT_ROOT / "data" / "raw" / "PMA" / "records_enriched.jsonl"


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/merge_pma_batch.py <batch_json_file>")
        sys.exit(1)

    batch_file = Path(sys.argv[1])
    if not batch_file.exists():
        print(f"ERROR: Batch file not found: {batch_file}")
        sys.exit(1)

    # Load original records indexed by member_id
    originals = {}
    with open(INPUT_FILE) as f:
        for line in f:
            r = json.loads(line.strip())
            mid = r.get("member_id", "")
            if mid and mid not in originals:
                originals[mid] = r

    # Load batch results
    with open(batch_file) as f:
        batch_data = json.load(f)

    # Handle both formats: raw array or {data: [...]} wrapper
    if isinstance(batch_data, dict) and "data" in batch_data:
        results = batch_data["data"]
    elif isinstance(batch_data, list):
        results = batch_data
    else:
        print("ERROR: Unexpected batch format")
        sys.exit(1)

    # Merge: start with original record, overlay scraped data
    merged = []
    for scraped in results:
        mid = scraped.get("member_id", "")
        original = originals.get(mid, {})
        record = dict(original)  # Start with original fields
        # Overlay scraped fields (skip _error)
        for k, v in scraped.items():
            if k.startswith("_"):
                continue
            if v is not None and v != "":
                record[k] = v
        merged.append(record)

    # Append to output
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        for r in merged:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Stats
    n_web = sum(1 for r in merged if r.get("website"))
    n_phone = sum(1 for r in merged if r.get("phone"))
    n_contacts = sum(1 for r in merged if r.get("contacts"))
    n_emails = sum(sum(1 for c in r.get("contacts", []) if c.get("email")) for r in merged)
    print(f"Merged {len(merged)} records. "
          f"Websites: {n_web}, Phones: {n_phone}, "
          f"Contacts: {n_contacts}, Emails: {n_emails}")


if __name__ == "__main__":
    main()
