"""
Merge all pipeline JSONL records, deduplicate, score, and export to CSV.

Usage:
    python scripts/merge_and_export.py
"""

import csv
import json
import os
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_EXPORTS = PROJECT_ROOT / "data" / "exports"

# All company record sources
COMPANY_SOURCES = [
    ("PMA", DATA_RAW / "PMA" / "records_smoke_test.jsonl"),
    ("NEMA", DATA_RAW / "NEMA" / "records_00280a42.jsonl"),
    ("AGMA", DATA_RAW / "AGMA" / "records.jsonl"),
    ("AGMA_live", DATA_RAW / "AGMA" / "records_live.jsonl"),
    ("SOCMA", DATA_RAW / "SOCMA" / "records.jsonl"),
    ("AIA", DATA_RAW / "AIA" / "records.jsonl"),
]

# Enriched sources (overrides raw for matching companies)
ENRICHED_SOURCES = [
    DATA_PROCESSED / "NEMA" / "enriched.jsonl",
]

# Supplemental data
EVENTS_FILE = DATA_RAW / "events" / "trade_shows.jsonl"
COMPETITORS_FILE = DATA_RAW / "intelligence" / "competitors.jsonl"
CONTACTS_FILE = DATA_RAW / "contacts" / "association_contacts.jsonl"

# Salesforce field mapping
SALESFORCE_MAPPING = {
    "company_name": "Account Name",
    "website": "Website",
    "domain": "Domain__c",
    "city": "BillingCity",
    "state": "BillingState",
    "country": "BillingCountry",
    "employee_count_min": "NumberOfEmployees",
    "revenue_min_usd": "AnnualRevenue",
    "industry": "Industry",
    "erp_system": "ERP_System__c",
    "associations": "Associations__c",
    "quality_score": "Data_Quality_Score__c",
    "member_type": "Member_Type__c",
    "sector": "Sector__c",
    "notes": "Notes__c",
    "tech_stack": "Tech_Stack__c",
}


def normalize_company_name(name):
    """Normalize company name for deduplication."""
    if not name:
        return ""
    name = name.strip()
    # Remove common suffixes for comparison
    suffixes = [
        ", Inc.", ", Inc", " Inc.", " Inc", ", LLC", " LLC",
        ", Ltd.", ", Ltd", " Ltd.", " Ltd", ", L.P.", " L.P.",
        " Corp.", " Corp", " Corporation", " Company", " Co.",
        ", Co.", " & Co.", " Group", " Holdings",
    ]
    normalized = name
    for suffix in suffixes:
        if normalized.endswith(suffix):
            normalized = normalized[:-len(suffix)]
            break
    return normalized.strip().lower()


def compute_quality_score(record):
    """Score a record 0-100 based on field completeness."""
    score = 0
    weights = {
        "company_name": 20,
        "website": 15,
        "domain": 10,
        "city": 5,
        "state": 5,
        "country": 3,
        "member_type": 3,
        "industry": 5,
        "sector": 5,
        "tech_stack": 10,
        "employee_count_min": 5,
        "revenue_min_usd": 5,
        "erp_system": 5,
        "contacts": 4,
    }
    for field, weight in weights.items():
        val = record.get(field)
        if val:
            if isinstance(val, list) and len(val) > 0:
                score += weight
            elif isinstance(val, str) and val.strip():
                score += weight
            elif isinstance(val, (int, float)) and val > 0:
                score += weight
    return min(score, 100)


def load_jsonl(path):
    """Load JSONL file, return list of dicts."""
    records = []
    if not path.exists():
        return records
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records


def merge_records(existing, new_record):
    """Merge new_record into existing, keeping non-empty fields."""
    for key, val in new_record.items():
        if key == "associations":
            continue  # handled separately
        if val and not existing.get(key):
            existing[key] = val
    return existing


def main():
    DATA_EXPORTS.mkdir(parents=True, exist_ok=True)

    # Step 1: Load all raw company records
    print("=" * 60)
    print("NAM Intelligence Pipeline - Merge & Export")
    print("=" * 60)

    all_records = []
    source_counts = {}

    for label, path in COMPANY_SOURCES:
        records = load_jsonl(path)
        source_counts[label] = len(records)
        all_records.extend(records)
        if records:
            print(f"  Loaded {len(records):>5} records from {label}")
        else:
            print(f"  Skipped {label} (file not found: {path.name})")

    print(f"\n  Total raw records: {len(all_records)}")

    # Step 2: Load enriched records (these override raw for matching companies)
    enriched_map = {}
    for path in ENRICHED_SOURCES:
        for rec in load_jsonl(path):
            key = normalize_company_name(rec.get("company_name", ""))
            if key:
                enriched_map[key] = rec

    print(f"  Enriched records available: {len(enriched_map)}")

    # Step 3: Deduplicate by normalized company name
    company_map = {}  # normalized_name -> merged record
    for rec in all_records:
        name = rec.get("company_name", "").strip()
        if not name:
            continue
        key = normalize_company_name(name)

        if key in company_map:
            existing = company_map[key]
            # Merge fields
            merge_records(existing, rec)
            # Track associations
            assoc = rec.get("association", "")
            if assoc and assoc not in existing.get("associations", []):
                existing.setdefault("associations", []).append(assoc)
        else:
            rec_copy = dict(rec)
            assoc = rec_copy.pop("association", "")
            rec_copy["associations"] = [assoc] if assoc else []
            company_map[key] = rec_copy

    # Step 4: Apply enrichment data
    enriched_count = 0
    for key, enriched in enriched_map.items():
        if key in company_map:
            merge_records(company_map[key], enriched)
            enriched_count += 1
        else:
            rec_copy = dict(enriched)
            assoc = rec_copy.pop("association", "")
            rec_copy["associations"] = [assoc] if assoc else []
            company_map[key] = rec_copy

    # Step 5: Compute quality scores
    for rec in company_map.values():
        rec["quality_score"] = compute_quality_score(rec)
        # Convert associations list to string for CSV
        if isinstance(rec.get("associations"), list):
            rec["associations_list"] = rec["associations"]
            rec["associations"] = "; ".join(rec["associations"])

    companies = sorted(company_map.values(), key=lambda r: r.get("quality_score", 0), reverse=True)
    total_unique = len(companies)

    print(f"\n  Unique companies after dedup: {total_unique}")
    print(f"  Companies with enrichment: {enriched_count}")

    # Step 6: Compute statistics
    assoc_counts = defaultdict(int)
    field_coverage = defaultdict(int)
    fields_to_track = [
        "company_name", "website", "domain", "city", "state",
        "country", "member_type", "industry", "sector",
        "tech_stack", "quality_score",
    ]

    multi_assoc = 0
    for rec in companies:
        for assoc in rec.get("associations_list", []):
            assoc_counts[assoc] += 1
        if len(rec.get("associations_list", [])) > 1:
            multi_assoc += 1
        for field in fields_to_track:
            val = rec.get(field)
            if val and (not isinstance(val, str) or val.strip()):
                field_coverage[field] += 1

    print(f"\n--- Companies per Association ---")
    for assoc, count in sorted(assoc_counts.items(), key=lambda x: -x[1]):
        print(f"  {assoc:>8}: {count:>5}")
    print(f"  {'MULTI':>8}: {multi_assoc:>5} (in 2+ associations)")

    print(f"\n--- Field Coverage ---")
    for field in fields_to_track:
        count = field_coverage.get(field, 0)
        pct = (count / total_unique * 100) if total_unique > 0 else 0
        print(f"  {field:>20}: {count:>5} ({pct:5.1f}%)")

    # Step 7: Export companies_all.csv
    all_fields = [
        "company_name", "associations", "website", "domain",
        "city", "state", "country", "member_type", "industry", "sector",
        "notes", "tech_stack", "enrichment_status", "quality_score",
        "source_url", "extracted_at",
    ]

    all_csv = DATA_EXPORTS / "companies_all.csv"
    with open(all_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_fields, extrasaction="ignore")
        writer.writeheader()
        for rec in companies:
            row = dict(rec)
            # Convert lists to strings
            if isinstance(row.get("tech_stack"), list):
                row["tech_stack"] = "; ".join(row["tech_stack"])
            writer.writerow(row)
    print(f"\n  Exported: {all_csv} ({total_unique} rows)")

    # Step 8: Export companies_salesforce.csv
    sf_csv = DATA_EXPORTS / "companies_salesforce.csv"
    sf_fields = list(SALESFORCE_MAPPING.values())
    with open(sf_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sf_fields, extrasaction="ignore")
        writer.writeheader()
        for rec in companies:
            sf_row = {}
            for src_field, sf_field in SALESFORCE_MAPPING.items():
                val = rec.get(src_field, "")
                if isinstance(val, list):
                    val = "; ".join(str(v) for v in val)
                sf_row[sf_field] = val
            writer.writerow(sf_row)
    print(f"  Exported: {sf_csv} ({total_unique} rows)")

    # Step 9: Export events_2026.csv
    events = load_jsonl(EVENTS_FILE)
    if events:
        events_csv = DATA_EXPORTS / "events_2026.csv"
        event_fields = [
            "event_name", "dates", "city", "venue", "attendance",
            "industry", "registration_url", "notes", "priority",
        ]
        with open(events_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=event_fields, extrasaction="ignore")
            writer.writeheader()
            for ev in events:
                writer.writerow(ev)
        print(f"  Exported: {events_csv} ({len(events)} events)")

    # Step 10: Export competitor_analysis.csv
    competitors = load_jsonl(COMPETITORS_FILE)
    if competitors:
        comp_csv = DATA_EXPORTS / "competitor_analysis.csv"
        comp_fields = ["competitor", "presence", "strategy_notes", "threat_level"]
        with open(comp_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=comp_fields, extrasaction="ignore")
            writer.writeheader()
            for comp in competitors:
                writer.writerow(comp)
        print(f"  Exported: {comp_csv} ({len(competitors)} competitors)")

    # Step 11: Export association_contacts.csv
    contacts = load_jsonl(CONTACTS_FILE)
    if contacts:
        contacts_csv = DATA_EXPORTS / "association_contacts.csv"
        contact_fields = ["organization", "name", "email", "phone", "notes"]
        with open(contacts_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=contact_fields, extrasaction="ignore")
            writer.writeheader()
            for c in contacts:
                writer.writerow(c)
        print(f"  Exported: {contacts_csv} ({len(contacts)} contacts)")

    # Final summary
    print(f"\n{'=' * 60}")
    print(f"FINAL SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Total unique companies:  {total_unique}")
    print(f"  Companies with website:  {field_coverage.get('website', 0)}")
    print(f"  Companies with domain:   {field_coverage.get('domain', 0)}")
    print(f"  Companies with city:     {field_coverage.get('city', 0)}")
    print(f"  Companies enriched:      {enriched_count}")
    print(f"  Multi-association:       {multi_assoc}")
    print(f"  Associations covered:    {len(assoc_counts)}")
    print(f"  Trade shows loaded:      {len(events)}")
    print(f"  Competitors tracked:     {len(competitors)}")
    print(f"  Association contacts:    {len(contacts)}")
    print(f"\n  Export files in: {DATA_EXPORTS}/")
    print(f"  Timestamp: {datetime.now(UTC).isoformat()}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
