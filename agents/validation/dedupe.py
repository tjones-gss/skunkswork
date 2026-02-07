"""
Dedupe Agent
NAM Intelligence Pipeline

Detects and merges duplicate company records.
"""

from collections import defaultdict
from datetime import UTC, datetime

from agents.base import BaseAgent
from skills.common.SKILL import extract_domain, normalize_company_name


class DedupeAgent(BaseAgent):
    """
    Dedupe Agent - merges duplicate records.

    Responsibilities:
    - Fuzzy match on company name
    - Exact match on domain
    - Address-based matching
    - Merge records keeping best data
    - Track duplicate groups
    """

    def _setup(self, **kwargs):
        """Initialize dedupe settings."""
        self.threshold = self.agent_config.get("threshold", 0.85)
        self.match_fields = self.agent_config.get("match_fields", [
            "company_name", "domain", "city", "state"
        ])
        self.weights = self.agent_config.get("weights", {
            "company_name": 0.5,
            "domain": 0.3,
            "city": 0.1,
            "state": 0.1
        })

    async def run(self, task: dict) -> dict:
        """
        Deduplicate records.

        Args:
            task: {
                "records": [{...}, ...]
            }

        Returns:
            {
                "success": True,
                "records": [{...merged...}, ...],
                "duplicates_found": 45,
                "duplicate_groups": [[1, 5, 12], ...]
            }
        """
        records = task.get("records", [])

        if not records:
            return {
                "success": False,
                "error": "No records provided",
                "records": [],
                "records_processed": 0
            }

        self.log.info(f"Deduplicating {len(records)} records")

        # Index records for fast lookup
        domain_index = defaultdict(list)
        name_index = defaultdict(list)

        for i, record in enumerate(records):
            domain = extract_domain(record.get("website", ""))
            if domain:
                domain_index[domain].append(i)

            normalized_name = normalize_company_name(record.get("company_name", ""))
            if normalized_name:
                # Index by first word for blocking
                first_word = normalized_name.split()[0] if normalized_name.split() else ""
                if first_word:
                    name_index[first_word].append(i)

        # Find duplicate groups
        duplicate_groups = []
        processed = set()

        for i, record in enumerate(records):
            if i in processed:
                continue

            group = [i]
            processed.add(i)

            # Get candidates to compare
            candidates = set()

            # Same domain = definite match candidates
            domain = extract_domain(record.get("website", ""))
            if domain:
                candidates.update(domain_index[domain])

            # Same first word in name = possible match candidates
            normalized_name = normalize_company_name(record.get("company_name", ""))
            first_word = normalized_name.split()[0] if normalized_name.split() else ""
            if first_word:
                candidates.update(name_index[first_word])

            # Compare with candidates
            for j in candidates:
                if j in processed or j == i:
                    continue

                similarity = self._calculate_similarity(record, records[j])

                if similarity >= self.threshold:
                    group.append(j)
                    processed.add(j)

            if len(group) > 1:
                duplicate_groups.append(group)

        # Merge duplicate groups
        merged_records = []
        merged_indices = set()

        for group in duplicate_groups:
            merged = self._merge_records([records[i] for i in group])
            merged["_duplicate_group"] = group
            merged_records.append(merged)
            merged_indices.update(group)

        # Add non-duplicate records
        for i, record in enumerate(records):
            if i not in merged_indices:
                merged_records.append(record)

        self.log.info(
            "Deduplication complete",
            original_count=len(records),
            final_count=len(merged_records),
            duplicates_merged=len(records) - len(merged_records),
            duplicate_groups=len(duplicate_groups)
        )

        return {
            "success": True,
            "records": merged_records,
            "duplicates_found": len(records) - len(merged_records),
            "duplicate_groups": duplicate_groups,
            "records_processed": len(records)
        }

    def _calculate_similarity(self, record1: dict, record2: dict) -> float:
        """Calculate similarity score between two records."""
        total_score = 0.0
        total_weight = 0.0

        for field, weight in self.weights.items():
            value1 = record1.get(field, "")
            value2 = record2.get(field, "")

            if not value1 or not value2:
                continue

            total_weight += weight

            if field == "company_name":
                # Fuzzy matching for company names
                similarity = self._fuzzy_match(
                    normalize_company_name(str(value1)),
                    normalize_company_name(str(value2))
                )
            elif field == "domain":
                # Exact match for domain
                d1 = extract_domain(str(value1))
                d2 = extract_domain(str(value2))
                similarity = 1.0 if d1 and d2 and d1 == d2 else 0.0
            else:
                # Case-insensitive exact match
                similarity = 1.0 if str(value1).lower() == str(value2).lower() else 0.0

            total_score += similarity * weight

        return total_score / total_weight if total_weight > 0 else 0.0

    def _fuzzy_match(self, s1: str, s2: str) -> float:
        """Calculate fuzzy string similarity."""
        if not s1 or not s2:
            return 0.0

        if s1 == s2:
            return 1.0

        # Try using rapidfuzz if available
        try:
            from rapidfuzz import fuzz
            return fuzz.ratio(s1, s2) / 100.0
        except ImportError:
            pass

        # Fallback to basic Levenshtein-like similarity
        return self._basic_similarity(s1, s2)

    def _basic_similarity(self, s1: str, s2: str) -> float:
        """Basic string similarity using edit distance."""
        if not s1 or not s2:
            return 0.0
        try:
            from rapidfuzz import fuzz
            return fuzz.ratio(s1.lower(), s2.lower()) / 100.0
        except ImportError:
            # Pure-Python fallback: positional character matching
            s1, s2 = s1.lower(), s2.lower()
            matches = sum(c1 == c2 for c1, c2 in zip(s1, s2, strict=False))
            return matches / max(len(s1), len(s2))

    def _merge_records(self, records: list[dict]) -> dict:
        """Merge multiple records into one, keeping best data."""
        if not records:
            return {}

        if len(records) == 1:
            return records[0].copy()

        # Start with first record as base
        merged = records[0].copy()

        # Track associations from all records
        all_associations = set()
        for record in records:
            assoc = record.get("association")
            if assoc:
                if isinstance(assoc, list):
                    all_associations.update(assoc)
                else:
                    all_associations.add(assoc)

        # Merge fields from other records
        for record in records[1:]:
            for key, value in record.items():
                if key.startswith("_"):
                    continue

                existing = merged.get(key)

                # If merged doesn't have this field, add it
                if not existing and value:
                    merged[key] = value
                    continue

                # For contacts, combine lists
                if key == "contacts" and isinstance(value, list):
                    existing_contacts = merged.get("contacts", [])
                    # Dedupe by email or name
                    existing_keys = {c.get("email") or c.get("name") for c in existing_contacts}
                    for contact in value:
                        key_val = contact.get("email") or contact.get("name")
                        if key_val and key_val not in existing_keys:
                            existing_contacts.append(contact)
                            existing_keys.add(key_val)
                    merged["contacts"] = existing_contacts
                    continue

                # For tech_stack, combine lists
                if key == "tech_stack" and isinstance(value, list):
                    existing_stack = merged.get("tech_stack", [])
                    for tech in value:
                        if tech not in existing_stack:
                            existing_stack.append(tech)
                    merged["tech_stack"] = existing_stack
                    continue

                # For numeric fields, keep higher value
                if key in ["employee_count_min", "employee_count_max", "revenue_min_usd", "quality_score"]:
                    if isinstance(value, (int, float)) and isinstance(existing, (int, float)):
                        merged[key] = max(value, existing)

        # Set combined associations
        if all_associations:
            merged["associations"] = sorted(list(all_associations))

        # Update metadata
        merged["merged_at"] = datetime.now(UTC).isoformat()
        merged["merged_from_count"] = len(records)

        return merged
