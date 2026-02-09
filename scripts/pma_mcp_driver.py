"""
PMA MCP Batch Driver — generates JS code for browser_run_code and saves results.

This orchestrates the MCP Playwright scraping by:
1. Reading batch files to get member IDs
2. Generating the JS code for browser_run_code
3. Saving results from each batch

Usage:
    python scripts/pma_mcp_driver.py ids BATCH_NUM      # Print IDs for a batch as JS array
    python scripts/pma_mcp_driver.py save BATCH_NUM     # Save results from stdin (JSON)
    python scripts/pma_mcp_driver.py next                # Show next unprocessed batch
    python scripts/pma_mcp_driver.py status              # Show overall progress
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BATCHES_DIR = PROJECT_ROOT / "data" / "raw" / "PMA" / "mcp_batches"
RESULTS_DIR = PROJECT_ROOT / "data" / "raw" / "PMA" / "mcp_results"


def get_batch_ids(batch_num):
    path = BATCHES_DIR / f"batch_{batch_num:04d}.json"
    if not path.exists():
        print(f"Batch file not found: {path}", file=sys.stderr)
        sys.exit(1)
    data = json.load(open(path))
    ids = [d["member_id"] for d in data]
    return ids


def print_ids(batch_num):
    ids = get_batch_ids(batch_num)
    print(json.dumps(ids))


def save_results(batch_num):
    """Read JSON results from stdin and save to results file."""
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    data = json.load(sys.stdin)
    # Accept either {results: [...]} or [...]
    if isinstance(data, dict) and "results" in data:
        results = data["results"]
    elif isinstance(data, list):
        results = data
    else:
        print(f"Unexpected data format", file=sys.stderr)
        sys.exit(1)

    path = RESULTS_DIR / f"result_{batch_num:04d}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    success = sum(1 for r in results if not r.get("error"))
    errors = sum(1 for r in results if r.get("error"))
    websites = sum(1 for r in results if r.get("website"))
    print(f"Saved {path.name}: {len(results)} records ({success} ok, {errors} err, {websites} websites)")


def next_batch():
    if not BATCHES_DIR.exists():
        print("No batches prepared. Run: python scripts/scrape_pma_mcp_batch.py prepare")
        return

    processed = set()
    if RESULTS_DIR.exists():
        for f in RESULTS_DIR.glob("result_*.json"):
            num = int(f.stem.split("_")[1])
            processed.add(num)

    all_batches = sorted([int(f.stem.split("_")[1]) for f in BATCHES_DIR.glob("batch_*.json")])
    pending = [b for b in all_batches if b not in processed]

    if not pending:
        print("All batches processed!")
        return

    next_num = pending[0]
    ids = get_batch_ids(next_num)
    print(f"Next batch: {next_num} ({len(ids)} profiles)")
    print(f"IDs: {json.dumps(ids)}")
    print(f"Remaining batches: {len(pending)}")


def show_status():
    if not BATCHES_DIR.exists():
        print("No batches prepared")
        return

    all_batches = sorted(BATCHES_DIR.glob("batch_*.json"))
    total_profiles = sum(len(json.load(open(f))) for f in all_batches)

    processed = 0
    total_success = 0
    total_errors = 0
    total_websites = 0
    total_phones = 0
    total_contacts = 0
    total_emails = 0

    if RESULTS_DIR.exists():
        for f in sorted(RESULTS_DIR.glob("result_*.json")):
            data = json.load(open(f))
            processed += len(data)
            total_success += sum(1 for r in data if not r.get("error"))
            total_errors += sum(1 for r in data if r.get("error"))
            total_websites += sum(1 for r in data if r.get("website"))
            total_phones += sum(1 for r in data if r.get("phone"))
            total_contacts += sum(1 for r in data if r.get("contacts"))
            total_emails += sum(sum(1 for c in r.get("contacts", []) if c.get("email")) for r in data)

    result_count = len(list(RESULTS_DIR.glob("result_*.json"))) if RESULTS_DIR.exists() else 0
    remaining = len(all_batches) - result_count

    print(f"PMA MCP Scraping Progress")
    print(f"{'='*50}")
    print(f"  Total profiles:   {total_profiles}")
    print(f"  Batches:          {result_count}/{len(all_batches)} ({result_count/len(all_batches)*100:.0f}%)")
    print(f"  Profiles done:    {processed}/{total_profiles} ({processed/total_profiles*100:.0f}%)")
    print(f"  Remaining:        {remaining} batches")
    print(f"")
    print(f"  Success:          {total_success} ({total_success/max(processed,1)*100:.0f}%)")
    print(f"  Errors:           {total_errors}")
    print(f"  Websites:         {total_websites} ({total_websites/max(total_success,1)*100:.0f}%)")
    print(f"  Phones:           {total_phones} ({total_phones/max(total_success,1)*100:.0f}%)")
    print(f"  With contacts:    {total_contacts} ({total_contacts/max(total_success,1)*100:.0f}%)")
    print(f"  Total emails:     {total_emails}")

    if remaining > 0:
        est_min = remaining * 50 / 60  # ~50s per batch
        print(f"\n  Est. time left:   ~{est_min:.0f} min")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/pma_mcp_driver.py [ids|save|next|status] [BATCH_NUM]")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "ids":
        print_ids(int(sys.argv[2]))
    elif cmd == "save":
        save_results(int(sys.argv[2]))
    elif cmd == "next":
        next_batch()
    elif cmd == "status":
        show_status()
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)
