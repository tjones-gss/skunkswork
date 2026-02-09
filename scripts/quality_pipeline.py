"""
Quality Pipeline - Fuzzy dedupe, 4-component scoring, and CSV export.

Replaces the naive merge_and_export.py with production-grade deduplication
using rapidfuzz and the ScorerAgent's 4-component quality model.

Usage:
    python scripts/quality_pipeline.py
"""

import csv
import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

from rapidfuzz import fuzz

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
DATA_EXPORTS = PROJECT_ROOT / "data" / "exports"

# ---------------------------------------------------------------------------
# Source file registry (checked for existence at load time)
# ---------------------------------------------------------------------------
COMPANY_SOURCES = [
    # PMA: prefer enriched if agent 1 produced it, else raw smoke test
    ("PMA", DATA_RAW / "PMA" / "records_enriched.jsonl"),
    ("PMA", DATA_RAW / "PMA" / "records_smoke_test.jsonl"),
    # NEMA enriched (agent 2 output or standalone)
    ("NEMA", DATA_PROCESSED / "enriched_all.jsonl"),
    ("NEMA", DATA_PROCESSED / "NEMA" / "enriched.jsonl"),
    ("NEMA", DATA_RAW / "NEMA" / "records_00280a42.jsonl"),
    # AGMA live + seed
    ("AGMA", DATA_RAW / "AGMA" / "records_live.jsonl"),
    ("AGMA", DATA_RAW / "AGMA" / "records.jsonl"),
    # SOCMA and AIA seeds
    ("SOCMA", DATA_RAW / "SOCMA" / "records.jsonl"),
    ("AIA", DATA_RAW / "AIA" / "records.jsonl"),
]

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

# ---------------------------------------------------------------------------
# Normalization helpers (mirrors skills/common/SKILL.py)
# ---------------------------------------------------------------------------
_SUFFIX_RE = re.compile(
    r'\b('
    r'inc\.?|incorporated|corp\.?|corporation|llc|l\.l\.c\.?|'
    r'ltd\.?|limited|co\.?|company|plc|'
    r'gmbh|ag|sa|nv|bv|'
    r'holdings|group'
    r')$',
    re.IGNORECASE,
)


def normalize_company_name(name: str) -> str:
    if not name:
        return ""
    name = name.lower().strip()
    name = _SUFFIX_RE.sub("", name)
    name = re.sub(r'[^\w\s]', '', name)
    return " ".join(name.split()).strip()


def extract_domain(url: str) -> str:
    if not url:
        return ""
    from urllib.parse import urlparse
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"
    domain = urlparse(url).netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


# ---------------------------------------------------------------------------
# JSONL I/O
# ---------------------------------------------------------------------------
def load_jsonl(path: Path) -> list[dict]:
    records = []
    if not path.exists():
        return records
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    return records


# ---------------------------------------------------------------------------
# Step 1: Load all records (de-duplicating sources per association)
# ---------------------------------------------------------------------------
def load_all_records() -> tuple[list[dict], dict[str, int]]:
    """Load company records, preferring richer files for each association."""
    all_records: list[dict] = []
    source_counts: dict[str, int] = {}
    loaded_assocs: set[str] = set()

    print("\n--- Loading Records ---")
    for assoc, path in COMPANY_SOURCES:
        if not path.exists():
            continue
        # For PMA and NEMA, only load the best available file (first match)
        # For AGMA, load both seed + live since they may have different companies
        if assoc in ("PMA", "NEMA") and assoc in loaded_assocs:
            continue
        # If enriched_all.jsonl exists, skip per-association enriched files
        if "enriched_all" in path.name and path.exists():
            # enriched_all has NEMA + AGMA enriched data
            records = load_jsonl(path)
            if records:
                all_records.extend(records)
                for r in records:
                    a = r.get("association", "UNKNOWN")
                    source_counts[a] = source_counts.get(a, 0) + 1
                    loaded_assocs.add(a)
                print(f"  Loaded {len(records):>5} records from {path.name} (multi-assoc)")
                continue

        records = load_jsonl(path)
        if records:
            all_records.extend(records)
            label = f"{assoc} ({path.name})"
            source_counts[label] = len(records)
            loaded_assocs.add(assoc)
            print(f"  Loaded {len(records):>5} records from {label}")

    print(f"\n  Total raw records loaded: {len(all_records)}")
    return all_records, source_counts


# ---------------------------------------------------------------------------
# Step 2: Fuzzy Deduplication
# ---------------------------------------------------------------------------
def fuzzy_dedupe(records: list[dict], threshold: float = 0.85) -> tuple[list[dict], int]:
    """Deduplicate records using rapidfuzz + domain matching.

    Blocking strategy: index by first word of normalized name for O(n*k)
    Similarity: company_name 50%, domain 30%, city+state 20%
    """
    weights = {"company_name": 0.5, "domain": 0.3, "city": 0.1, "state": 0.1}

    # Build indices for blocking
    domain_index: dict[str, list[int]] = defaultdict(list)
    name_index: dict[str, list[int]] = defaultdict(list)

    for i, rec in enumerate(records):
        domain = extract_domain(rec.get("website", "") or rec.get("domain", ""))
        if domain:
            domain_index[domain].append(i)
        norm = normalize_company_name(rec.get("company_name", ""))
        if norm:
            first_word = norm.split()[0] if norm.split() else ""
            if first_word:
                name_index[first_word].append(i)

    # Find duplicate groups
    processed: set[int] = set()
    duplicate_groups: list[list[int]] = []

    for i in range(len(records)):
        if i in processed:
            continue
        group = [i]
        processed.add(i)

        rec = records[i]
        candidates: set[int] = set()

        domain = extract_domain(rec.get("website", "") or rec.get("domain", ""))
        if domain:
            candidates.update(domain_index[domain])

        norm = normalize_company_name(rec.get("company_name", ""))
        first_word = norm.split()[0] if norm.split() else ""
        if first_word:
            candidates.update(name_index[first_word])

        for j in candidates:
            if j in processed or j == i:
                continue
            sim = _calculate_similarity(rec, records[j], weights)
            if sim >= threshold:
                group.append(j)
                processed.add(j)

        if len(group) > 1:
            duplicate_groups.append(group)

    # Merge duplicate groups
    merged_records: list[dict] = []
    merged_indices: set[int] = set()

    for group in duplicate_groups:
        merged = _merge_records([records[idx] for idx in group])
        merged_records.append(merged)
        merged_indices.update(group)

    # Add non-duplicate records
    for i, rec in enumerate(records):
        if i not in merged_indices:
            # Normalise associations field
            assoc = rec.pop("association", None)
            if assoc and "associations" not in rec:
                rec["associations"] = [assoc] if isinstance(assoc, str) else assoc
            elif assoc and isinstance(rec.get("associations"), list):
                if assoc not in rec["associations"]:
                    rec["associations"].append(assoc)
            merged_records.append(rec)

    duplicates_merged = len(records) - len(merged_records)
    return merged_records, duplicates_merged


def _calculate_similarity(r1: dict, r2: dict, weights: dict) -> float:
    total_score = 0.0
    total_weight = 0.0

    for field, weight in weights.items():
        v1 = r1.get(field, "") or ""
        v2 = r2.get(field, "") or ""

        if field == "company_name":
            v1 = normalize_company_name(str(v1))
            v2 = normalize_company_name(str(v2))
        elif field == "domain":
            v1 = extract_domain(str(r1.get("website", "") or r1.get("domain", "")))
            v2 = extract_domain(str(r2.get("website", "") or r2.get("domain", "")))

        if not v1 or not v2:
            continue

        total_weight += weight

        if field == "company_name":
            total_score += (fuzz.ratio(v1, v2) / 100.0) * weight
        elif field == "domain":
            total_score += (1.0 if v1 == v2 else 0.0) * weight
        else:
            total_score += (1.0 if str(v1).lower() == str(v2).lower() else 0.0) * weight

    return total_score / total_weight if total_weight > 0 else 0.0


def _merge_records(records: list[dict]) -> dict:
    """Merge duplicates keeping best data from each. Mirrors DedupeAgent._merge_records."""
    if not records:
        return {}
    if len(records) == 1:
        merged = records[0].copy()
        assoc = merged.pop("association", None)
        merged["associations"] = [assoc] if assoc else []
        return merged

    merged = records[0].copy()

    all_associations: set[str] = set()
    for rec in records:
        assoc = rec.get("association")
        if assoc:
            if isinstance(assoc, list):
                all_associations.update(assoc)
            else:
                all_associations.add(assoc)
        for a in rec.get("associations", []):
            if a:
                all_associations.add(a)

    for rec in records[1:]:
        for key, value in rec.items():
            if key.startswith("_") or key in ("association", "associations"):
                continue
            existing = merged.get(key)

            if not existing and value:
                merged[key] = value
                continue

            if key == "contacts" and isinstance(value, list):
                existing_contacts = merged.get("contacts", [])
                existing_keys = {c.get("email") or c.get("name") for c in existing_contacts}
                for contact in value:
                    key_val = contact.get("email") or contact.get("name")
                    if key_val and key_val not in existing_keys:
                        existing_contacts.append(contact)
                        existing_keys.add(key_val)
                merged["contacts"] = existing_contacts
                continue

            if key == "tech_stack" and isinstance(value, list):
                existing_stack = merged.get("tech_stack", [])
                for tech in value:
                    if tech not in existing_stack:
                        existing_stack.append(tech)
                merged["tech_stack"] = existing_stack
                continue

            if key in ("employee_count_min", "employee_count_max",
                       "revenue_min_usd", "revenue_max_usd"):
                if isinstance(value, (int, float)) and isinstance(existing, (int, float)):
                    merged[key] = max(value, existing)

    merged.pop("association", None)
    merged["associations"] = sorted(all_associations) if all_associations else []
    merged["merged_from_count"] = len(records)
    return merged


# ---------------------------------------------------------------------------
# Step 3: Quality Scoring (mirrors ScorerAgent)
# ---------------------------------------------------------------------------
REQUIRED_FIELDS = ["company_name", "website", "city", "state"]
VALUABLE_FIELDS = [
    "employee_count_min", "revenue_min_usd", "erp_system", "contacts",
    "year_founded", "naics_code", "industry", "phone", "email",
]
SCORE_WEIGHTS = {
    "completeness": 0.30,
    "accuracy": 0.40,
    "freshness": 0.15,
    "source_reliability": 0.15,
}
SOURCE_SCORES = {
    "clearbit": 95, "zoominfo": 90, "apollo": 85, "builtwith": 80,
    "website": 70, "website_scrape": 70, "job_postings": 65,
    "association": 60, "xlsx_seed": 55, "unknown": 50,
}


def _has_value(value) -> bool:
    if value is None:
        return False
    if isinstance(value, str) and not value.strip():
        return False
    if isinstance(value, list) and len(value) == 0:
        return False
    return True


def score_completeness(rec: dict) -> float:
    req_filled = sum(1 for f in REQUIRED_FIELDS if _has_value(rec.get(f)))
    req_score = (req_filled / len(REQUIRED_FIELDS)) * 60
    val_filled = sum(1 for f in VALUABLE_FIELDS if _has_value(rec.get(f)))
    val_score = (val_filled / len(VALUABLE_FIELDS)) * 40
    return req_score + val_score


def score_accuracy(rec: dict) -> float:
    score = 60.0
    domain = extract_domain(rec.get("website", "") or rec.get("domain", ""))
    if domain:
        score += 10
    if rec.get("enrichment_status") == "complete":
        score += 10
    validation = rec.get("_validation", {})
    if validation.get("dns_mx_valid") is True:
        score += 15
    elif validation.get("dns_mx_valid") is False:
        score -= 15
    return max(0, min(100, score))


def score_freshness(rec: dict) -> float:
    extracted_at = rec.get("extracted_at")
    if not extracted_at:
        return 50.0
    try:
        if isinstance(extracted_at, str):
            dt = datetime.fromisoformat(extracted_at.replace("Z", "+00:00"))
        else:
            dt = extracted_at
        now = datetime.now(dt.tzinfo) if dt.tzinfo else datetime.now(UTC)
        days = (now - dt).days
        if days <= 7:
            return 100.0
        if days <= 30:
            return 90.0
        if days <= 90:
            return 75.0
        if days <= 180:
            return 60.0
        if days <= 365:
            return 40.0
        return 20.0
    except Exception:
        return 50.0


def score_source_reliability(rec: dict) -> float:
    sources: list[float] = []
    for src_key in ("firmographic_source", "tech_source", "enrichment_source"):
        src = rec.get(src_key)
        if src:
            sources.append(SOURCE_SCORES.get(src, 50))
    for assoc in rec.get("associations", []):
        if assoc:
            sources.append(SOURCE_SCORES.get("association", 60))
    source_url = rec.get("source_url", "")
    if source_url and "xlsx_seed" in source_url:
        sources.append(SOURCE_SCORES.get("xlsx_seed", 55))
    if not sources:
        return 50.0
    base = sum(sources) / len(sources)
    bonus = min(10, len(sources) * 2)
    return min(100, base + bonus)


def get_grade(score: int) -> str:
    if score >= 90:
        return "A"
    if score >= 80:
        return "B"
    if score >= 70:
        return "C"
    if score >= 60:
        return "D"
    return "F"


def score_records(records: list[dict]) -> dict:
    """Score all records, return quality distribution stats."""
    dist = {"A": 0, "B": 0, "C": 0, "D": 0, "F": 0}
    scores: list[int] = []

    for rec in records:
        comp = score_completeness(rec)
        acc = score_accuracy(rec)
        fresh = score_freshness(rec)
        src = score_source_reliability(rec)

        final = (
            comp * SCORE_WEIGHTS["completeness"]
            + acc * SCORE_WEIGHTS["accuracy"]
            + fresh * SCORE_WEIGHTS["freshness"]
            + src * SCORE_WEIGHTS["source_reliability"]
        )
        final = max(0, min(100, round(final)))
        grade = get_grade(final)

        rec["quality_score"] = final
        rec["quality_grade"] = grade
        dist[grade] += 1
        scores.append(final)

    avg = sum(scores) / len(scores) if scores else 0
    return {"distribution": dist, "average": round(avg, 1), "scores": scores}


# ---------------------------------------------------------------------------
# Step 4: CSV Export
# ---------------------------------------------------------------------------
ALL_CSV_FIELDS = [
    "company_name", "associations", "website", "domain",
    "city", "state", "country", "member_type", "industry", "sector",
    "notes", "tech_stack", "enrichment_status", "quality_score", "quality_grade",
    "source_url", "extracted_at",
]


def _prep_row(rec: dict) -> dict:
    """Prepare a record dict for CSV writing (lists -> semicolon strings)."""
    row = dict(rec)
    if isinstance(row.get("associations"), list):
        row["associations"] = "; ".join(row["associations"])
    if isinstance(row.get("tech_stack"), list):
        row["tech_stack"] = "; ".join(row["tech_stack"])
    if isinstance(row.get("contacts"), list):
        row["contacts"] = "; ".join(
            c.get("name", "") or c.get("email", "") for c in row["contacts"]
        )
    return row


def export_csvs(companies: list[dict]) -> dict[str, int]:
    """Write all CSV exports. Returns {filename: row_count}."""
    DATA_EXPORTS.mkdir(parents=True, exist_ok=True)
    exported: dict[str, int] = {}

    # 1. companies_all.csv
    path = DATA_EXPORTS / "companies_all.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ALL_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for rec in companies:
            writer.writerow(_prep_row(rec))
    exported["companies_all.csv"] = len(companies)

    # 2. companies_salesforce.csv
    sf_fields = list(SALESFORCE_MAPPING.values())
    path = DATA_EXPORTS / "companies_salesforce.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sf_fields, extrasaction="ignore")
        writer.writeheader()
        for rec in companies:
            prepped = _prep_row(rec)
            sf_row = {}
            for src_field, sf_field in SALESFORCE_MAPPING.items():
                val = prepped.get(src_field, "")
                sf_row[sf_field] = val
            writer.writerow(sf_row)
    exported["companies_salesforce.csv"] = len(companies)

    # 3. companies_high_quality.csv (B+ grade, score >= 80)
    high_q = [r for r in companies if r.get("quality_score", 0) >= 80]
    path = DATA_EXPORTS / "companies_high_quality.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=ALL_CSV_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for rec in high_q:
            writer.writerow(_prep_row(rec))
    exported["companies_high_quality.csv"] = len(high_q)

    # 4. events_2026.csv
    events = load_jsonl(EVENTS_FILE)
    if events:
        event_fields = [
            "event_name", "dates", "city", "venue", "attendance",
            "industry", "registration_url", "notes", "priority",
        ]
        path = DATA_EXPORTS / "events_2026.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=event_fields, extrasaction="ignore")
            writer.writeheader()
            for ev in events:
                writer.writerow(ev)
        exported["events_2026.csv"] = len(events)

    # 5. competitor_analysis.csv
    competitors = load_jsonl(COMPETITORS_FILE)
    if competitors:
        comp_fields = ["competitor", "presence", "strategy_notes", "threat_level"]
        path = DATA_EXPORTS / "competitor_analysis.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=comp_fields, extrasaction="ignore")
            writer.writeheader()
            for comp in competitors:
                writer.writerow(comp)
        exported["competitor_analysis.csv"] = len(competitors)

    # 6. association_contacts.csv
    contacts = load_jsonl(CONTACTS_FILE)
    if contacts:
        contact_fields = ["organization", "name", "email", "phone", "notes"]
        path = DATA_EXPORTS / "association_contacts.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=contact_fields, extrasaction="ignore")
            writer.writeheader()
            for c in contacts:
                writer.writerow(c)
        exported["association_contacts.csv"] = len(contacts)

    return exported


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 64)
    print("  NAM Intelligence Pipeline - Quality Pipeline")
    print("  Fuzzy Dedupe + 4-Component Scoring + Export")
    print("=" * 64)

    # Step 1: Load
    all_records, source_counts = load_all_records()
    if not all_records:
        print("\n  ERROR: No records found. Exiting.")
        return

    # Step 2: Fuzzy dedupe
    print("\n--- Fuzzy Deduplication (rapidfuzz, threshold=0.85) ---")
    deduped, duplicates_merged = fuzzy_dedupe(all_records, threshold=0.85)
    print(f"  Input records:     {len(all_records)}")
    print(f"  Duplicates merged: {duplicates_merged}")
    print(f"  Unique companies:  {len(deduped)}")

    # Ensure domain is populated from website
    for rec in deduped:
        if not rec.get("domain") and rec.get("website"):
            rec["domain"] = extract_domain(rec["website"])

    # Step 3: Quality scoring
    print("\n--- Quality Scoring (4-component model) ---")
    stats = score_records(deduped)
    dist = stats["distribution"]
    print(f"  Average score: {stats['average']}")
    for grade in ("A", "B", "C", "D", "F"):
        pct = (dist[grade] / len(deduped) * 100) if deduped else 0
        print(f"    Grade {grade}: {dist[grade]:>5} ({pct:5.1f}%)")

    # Sort by quality score descending
    deduped.sort(key=lambda r: r.get("quality_score", 0), reverse=True)

    # Step 4: Export
    print("\n--- Exporting CSVs ---")
    exported = export_csvs(deduped)
    for filename, count in exported.items():
        print(f"  {filename}: {count} rows")

    # Step 5: Summary
    assoc_counts: dict[str, int] = defaultdict(int)
    field_coverage: dict[str, int] = defaultdict(int)
    coverage_fields = [
        "website", "domain", "city", "state", "tech_stack",
        "industry", "employee_count_min", "revenue_min_usd",
    ]

    for rec in deduped:
        for a in rec.get("associations", []):
            if a:
                assoc_counts[a] += 1
        for f in coverage_fields:
            if _has_value(rec.get(f)):
                field_coverage[f] += 1

    high_q_count = dist.get("A", 0) + dist.get("B", 0)

    print(f"\n{'=' * 64}")
    print("  QUALITY PIPELINE RESULTS")
    print(f"{'=' * 64}")
    print(f"  Total unique companies: {len(deduped)}")
    print(f"  Companies per association:")
    for assoc, count in sorted(assoc_counts.items(), key=lambda x: -x[1]):
        print(f"    {assoc:>8}: {count}")
    print(f"  Duplicates merged: {duplicates_merged}")
    print(f"  Field coverage:")
    for f in coverage_fields:
        c = field_coverage.get(f, 0)
        pct = (c / len(deduped) * 100) if deduped else 0
        print(f"    {f:>20}: {c:>5} ({pct:5.1f}%)")
    print(f"  Quality distribution: A={dist['A']}, B={dist['B']}, "
          f"C={dist['C']}, D={dist['D']}, F={dist['F']}")
    print(f"  Average quality score: {stats['average']}")
    print(f"  High-quality records (B+): {high_q_count}")
    print(f"\n  Export directory: {DATA_EXPORTS}")
    print(f"  Timestamp: {datetime.now(UTC).isoformat()}")
    print(f"{'=' * 64}")


if __name__ == "__main__":
    main()
