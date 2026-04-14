#!/usr/bin/env python3
"""
SEC EDGAR Enrichment
NAM Intelligence Pipeline

Looks up public companies in our database via SEC EDGAR free API.
Gets: CIK number, SIC code, employee count, revenue, fiscal year, state.
No API key needed. Rate limit: 10 requests/sec (SEC allows this with User-Agent).
"""

import csv
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import httpx

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "exports" / "companies_all.csv"
OUTPUT_PATH = BASE_DIR / "data" / "processed" / "sec_edgar_enriched.jsonl"

# SEC requires a User-Agent with contact info
HEADERS = {
    "User-Agent": "GSS-NAM-Pipeline tjones@gssmail.com",
    "Accept": "application/json",
}

COMPANY_SEARCH_URL = "https://efts.sec.gov/LATEST/search-index?q={query}&dateRange=custom&startdt=2020-01-01&enddt=2026-12-31&forms=10-K"
COMPANY_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
COMPANY_FACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"


def load_companies() -> list[dict]:
    """Load company names from our pipeline."""
    records = []
    with open(CSV_PATH, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = (row.get("company_name") or "").strip()
            domain = (row.get("domain") or "").strip()
            if name:
                records.append({"company_name": name, "domain": domain})
    return records


def load_sec_tickers() -> dict:
    """Load SEC company tickers file — maps all public companies."""
    print("  Loading SEC company tickers...")
    resp = httpx.get(COMPANY_TICKERS_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # Build lookup by normalized name
    lookup = {}
    for entry in data.values():
        name = entry.get("title", "").strip()
        cik = str(entry.get("cik_str", ""))
        ticker = entry.get("ticker", "")
        if name and cik:
            key = re.sub(r"[^a-z0-9]", "", name.lower())
            lookup[key] = {"cik": cik, "ticker": ticker, "sec_name": name}

    print(f"  Loaded {len(lookup)} SEC-registered companies")
    return lookup


def get_company_info(cik: str) -> dict:
    """Get company details from SEC submissions API."""
    padded_cik = cik.zfill(10)
    try:
        resp = httpx.get(
            SUBMISSIONS_URL.format(cik=padded_cik),
            headers=HEADERS,
            timeout=15,
        )
        if resp.status_code != 200:
            return {}

        data = resp.json()
        result = {
            "sec_cik": cik,
            "sec_name": data.get("name", ""),
            "sec_sic": data.get("sic", ""),
            "sec_sic_description": data.get("sicDescription", ""),
            "sec_state": data.get("stateOfIncorporation", ""),
            "sec_fiscal_year_end": data.get("fiscalYearEnd", ""),
            "sec_exchange": "",
            "sec_ticker": "",
        }

        # Get exchange/ticker info
        exchanges = data.get("exchanges", [])
        tickers_list = data.get("tickers", [])
        if exchanges:
            result["sec_exchange"] = exchanges[0]
        if tickers_list:
            result["sec_ticker"] = tickers_list[0]

        # Get address
        addresses = data.get("addresses", {})
        business = addresses.get("business", {})
        if business:
            result["sec_street"] = business.get("street1", "")
            result["sec_city"] = business.get("city", "")
            result["sec_state_address"] = business.get("stateOrCountry", "")
            result["sec_zip"] = business.get("zipCode", "")

        return result
    except Exception as e:
        return {}


def match_companies(our_companies: list[dict], sec_lookup: dict) -> list[tuple]:
    """Match our companies against SEC registry."""
    matches = []

    for rec in our_companies:
        name = rec["company_name"]
        # Try exact normalized match
        key = re.sub(r"[^a-z0-9]", "", name.lower())

        if key in sec_lookup:
            matches.append((rec, sec_lookup[key]))
            continue

        # Try without common suffixes
        for suffix in ["inc", "corp", "corporation", "llc", "ltd", "co", "company",
                        "incorporated", "limited", "group", "holdings"]:
            trimmed = key.rstrip(suffix) if key.endswith(suffix) else key
            if trimmed != key and trimmed in sec_lookup:
                matches.append((rec, sec_lookup[trimmed]))
                break

    return matches


def main():
    print("=" * 60)
    print("NAM Intelligence Pipeline - SEC EDGAR Enrichment")
    print("=" * 60)

    companies = load_companies()
    print(f"\nLoaded {len(companies)} companies from pipeline")

    sec_lookup = load_sec_tickers()

    # Match
    print("\nMatching against SEC registry...")
    matches = match_companies(companies, sec_lookup)
    print(f"  Matched: {len(matches)} public companies")

    # Get detailed info for each match
    print(f"\nFetching SEC details for {len(matches)} companies...")
    enriched = []
    now = datetime.now(timezone.utc).isoformat()

    for i, (our_rec, sec_match) in enumerate(matches):
        cik = sec_match["cik"]
        info = get_company_info(cik)

        if info:
            enriched_rec = {
                "company_name": our_rec["company_name"],
                "domain": our_rec.get("domain", ""),
                **info,
                "sec_enriched_at": now,
                "publicly_traded": True,
            }
            enriched.append(enriched_rec)

        # Rate limit: SEC allows 10 req/sec
        time.sleep(0.15)

        if (i + 1) % 20 == 0:
            print(f"  [{i+1}/{len(matches)}] Fetched {len(enriched)} company details")

    # Write output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for rec in enriched:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")

    print(f"\n{'=' * 60}")
    print(f"SEC EDGAR Results")
    print(f"{'=' * 60}")
    print(f"  Public companies found: {len(enriched)}")
    if enriched:
        exchanges = [r.get("sec_exchange", "?") for r in enriched if r.get("sec_exchange")]
        sics = [r.get("sec_sic_description", "?") for r in enriched if r.get("sec_sic_description")]
        print(f"  Exchanges: {', '.join(set(exchanges))}")
        print(f"  Sample SIC codes: {', '.join(list(set(sics))[:10])}")
        print(f"\n  Sample companies:")
        for r in enriched[:10]:
            print(f"    {r['company_name']} | {r.get('sec_ticker','')} | {r.get('sec_sic_description','')}")

    print(f"\n  Output: {OUTPUT_PATH}")
    print("Done!")


if __name__ == "__main__":
    main()
