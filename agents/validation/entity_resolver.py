"""
Entity Resolver Agent
NAM Intelligence Pipeline

Consolidates extracted company records into canonical entities,
resolves duplicates, and maintains alias mappings.
"""

import re
from collections import defaultdict
from datetime import datetime, UTC
from typing import Optional

from agents.base import BaseAgent
from models.ontology import Company, Provenance
from middleware.policy import validate_json_output
from skills.common.SKILL import normalize_company_name, extract_domain


class EntityResolverAgent(BaseAgent):
    """
    Entity Resolver Agent - deduplicates and resolves company entities.

    Responsibilities:
    - Name normalization (legal suffixes, abbreviations)
    - Multi-signal matching (domain, name, phone, address)
    - Confidence scoring (0-100)
    - Merge with provenance tracking
    """

    # Legal suffix patterns for normalization
    LEGAL_SUFFIXES = [
        r'\b(inc\.?|incorporated)$',
        r'\b(corp\.?|corporation)$',
        r'\b(llc|l\.l\.c\.?)$',
        r'\b(ltd\.?|limited)$',
        r'\b(co\.?|company)$',
        r'\b(plc)$',
        r'\b(gmbh|ag|sa|nv|bv)$',
        r'\b(lp|l\.p\.)$',
        r'\b(llp|l\.l\.p\.)$',
    ]

    # Common abbreviations
    ABBREVIATIONS = {
        'mfg': 'manufacturing',
        'intl': 'international',
        'corp': 'corporation',
        'ind': 'industries',
        'mach': 'machine',
        'eng': 'engineering',
        'tech': 'technology',
        'svcs': 'services',
        'sys': 'systems',
        'assoc': 'associates',
    }

    def _setup(self, **kwargs):
        """Initialize resolver settings."""
        self.match_threshold = self.agent_config.get("match_threshold", 0.85)
        self.weights = self.agent_config.get("weights", {
            "domain": 0.40,
            "name": 0.35,
            "phone": 0.10,
            "address": 0.15
        })

    @validate_json_output
    async def run(self, task: dict) -> dict:
        """
        Resolve and deduplicate company records.

        Args:
            task: {
                "records": [{company_dict}, ...],
                "existing_entities": [{canonical_entity}, ...],  # Optional
                "merge_strategy": "keep_best" | "merge_all"  # Optional
            }

        Returns:
            {
                "success": True,
                "canonical_entities": [{...}, ...],
                "alias_mappings": {canonical_id: [aliases], ...},
                "merge_groups": [[record_ids], ...],
                "records_processed": 100
            }
        """
        records = task.get("records", [])
        existing_entities = task.get("existing_entities", [])
        merge_strategy = task.get("merge_strategy", "keep_best")

        if not records:
            return {
                "success": False,
                "error": "No records provided",
                "records": [],
                "records_processed": 0
            }

        self.log.info(f"Resolving {len(records)} records")

        # Build indexes for efficient matching
        domain_index = defaultdict(list)
        name_index = defaultdict(list)
        phone_index = defaultdict(list)

        # Index existing entities
        for i, entity in enumerate(existing_entities):
            self._index_record(entity, f"existing_{i}", domain_index, name_index, phone_index)

        # Index new records
        for i, record in enumerate(records):
            self._index_record(record, f"new_{i}", domain_index, name_index, phone_index)

        # Find matching groups
        all_records = existing_entities + records
        merge_groups = self._find_merge_groups(
            all_records, domain_index, name_index, phone_index,
            existing_count=len(existing_entities),
        )

        # Create canonical entities
        canonical_entities = []
        alias_mappings = {}
        processed_indices = set()

        for group in merge_groups:
            if len(group) == 1:
                idx = group[0]
                if idx < len(existing_entities):
                    canonical = existing_entities[idx]
                else:
                    canonical = records[idx - len(existing_entities)]
                canonical_entities.append(canonical)
            else:
                # Merge group
                group_records = [
                    existing_entities[i] if i < len(existing_entities)
                    else records[i - len(existing_entities)]
                    for i in group
                ]

                canonical, aliases = self._merge_group(group_records, merge_strategy)
                canonical_entities.append(canonical)

                if aliases:
                    alias_mappings[canonical.get("id", canonical.get("company_name"))] = aliases

            processed_indices.update(group)

        # Add unprocessed records
        for i, record in enumerate(records):
            adj_idx = i + len(existing_entities)
            if adj_idx not in processed_indices:
                canonical_entities.append(record)

        self.log.info(
            f"Resolution complete",
            input_records=len(records),
            canonical_entities=len(canonical_entities),
            merge_groups=len([g for g in merge_groups if len(g) > 1])
        )

        return {
            "success": True,
            "records": canonical_entities,
            "canonical_entities": canonical_entities,
            "alias_mappings": alias_mappings,
            "merge_groups": merge_groups,
            "records_processed": len(records)
        }

    def _index_record(
        self,
        record: dict,
        record_id: str,
        domain_index: dict,
        name_index: dict,
        phone_index: dict
    ):
        """Index a record for efficient matching."""
        # Index by domain
        domain = extract_domain(record.get("website", "") or record.get("domain", ""))
        if domain:
            domain_index[domain].append(record_id)

        # Index by normalized name (first word for blocking)
        name = normalize_company_name(record.get("company_name", ""))
        if name:
            first_word = name.split()[0] if name.split() else ""
            if first_word and len(first_word) > 2:
                name_index[first_word].append(record_id)

        # Index by phone (normalized)
        phone = self._normalize_phone(record.get("phone", ""))
        if phone and len(phone) >= 10:
            phone_index[phone[-10:]].append(record_id)  # Last 10 digits

    def _find_merge_groups(
        self,
        records: list[dict],
        domain_index: dict,
        name_index: dict,
        phone_index: dict,
        existing_count: int = 0,
    ) -> list[list[int]]:
        """Find groups of records that should be merged."""
        record_map = {}
        for index_dict in (domain_index, name_index, phone_index):
            for _key, id_list in index_dict.items():
                for str_id in id_list:
                    if str_id in record_map:
                        continue
                    prefix, num_str = str_id.split("_", 1)
                    num = int(num_str)
                    record_map[str_id] = num if prefix == "existing" else existing_count + num

        groups = []
        processed = set()

        for i, record in enumerate(records):
            if i in processed:
                continue

            group = [i]
            processed.add(i)

            # Find candidates
            candidates = set()

            # Same domain = strong match
            domain = extract_domain(record.get("website", "") or record.get("domain", ""))
            if domain and domain in domain_index:
                for idx in domain_index[domain]:
                    rec_idx = record_map.get(idx, -1)
                    if rec_idx >= 0 and rec_idx != i:
                        candidates.add(rec_idx)

            # Same first word in name = potential match
            name = normalize_company_name(record.get("company_name", ""))
            first_word = name.split()[0] if name and name.split() else ""
            if first_word and first_word in name_index:
                for idx in name_index[first_word]:
                    rec_idx = record_map.get(idx, -1)
                    if rec_idx >= 0 and rec_idx != i:
                        candidates.add(rec_idx)

            # Same phone = strong match
            phone = self._normalize_phone(record.get("phone", ""))
            if phone and len(phone) >= 10:
                phone_key = phone[-10:]
                if phone_key in phone_index:
                    for idx in phone_index[phone_key]:
                        rec_idx = record_map.get(idx, -1)
                        if rec_idx >= 0 and rec_idx != i:
                            candidates.add(rec_idx)

            # Score candidates
            for candidate_idx in candidates:
                if candidate_idx in processed:
                    continue

                score = self._calculate_match_score(record, records[candidate_idx])

                if score >= self.match_threshold:
                    group.append(candidate_idx)
                    processed.add(candidate_idx)

            groups.append(group)

        return groups

    def _calculate_match_score(self, record1: dict, record2: dict) -> float:
        """Calculate match score between two records."""
        total_score = 0.0
        total_weight = 0.0

        # Domain match (strongest signal)
        domain1 = extract_domain(record1.get("website", "") or record1.get("domain", ""))
        domain2 = extract_domain(record2.get("website", "") or record2.get("domain", ""))

        if domain1 and domain2:
            total_weight += self.weights["domain"]
            if domain1 == domain2:
                total_score += self.weights["domain"]

        # Name match
        name1 = self._deep_normalize_name(record1.get("company_name", ""))
        name2 = self._deep_normalize_name(record2.get("company_name", ""))

        if name1 and name2:
            total_weight += self.weights["name"]
            name_similarity = self._string_similarity(name1, name2)
            total_score += self.weights["name"] * name_similarity

        # Phone match
        phone1 = self._normalize_phone(record1.get("phone", ""))
        phone2 = self._normalize_phone(record2.get("phone", ""))

        if phone1 and phone2 and len(phone1) >= 10 and len(phone2) >= 10:
            total_weight += self.weights["phone"]
            if phone1[-10:] == phone2[-10:]:
                total_score += self.weights["phone"]

        # Address match
        addr1 = self._normalize_address(record1)
        addr2 = self._normalize_address(record2)

        if addr1 and addr2:
            total_weight += self.weights["address"]
            addr_similarity = self._string_similarity(addr1, addr2)
            total_score += self.weights["address"] * addr_similarity

        return total_score / total_weight if total_weight > 0 else 0.0

    def _deep_normalize_name(self, name: str) -> str:
        """Deep normalization of company name."""
        if not name:
            return ""

        name = name.lower().strip()

        # Remove legal suffixes
        for pattern in self.LEGAL_SUFFIXES:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)

        # Expand abbreviations
        words = name.split()
        expanded = []
        for word in words:
            expanded.append(self.ABBREVIATIONS.get(word, word))
        name = ' '.join(expanded)

        # Remove punctuation
        name = re.sub(r'[^\w\s]', '', name)

        # Normalize whitespace
        name = ' '.join(name.split())

        return name

    def _normalize_phone(self, phone: str) -> str:
        """Normalize phone number to digits only."""
        if not phone:
            return ""
        return re.sub(r'[^\d]', '', phone)

    def _normalize_address(self, record: dict) -> str:
        """Normalize address from record fields."""
        parts = []

        if record.get("city"):
            parts.append(record["city"].lower())

        if record.get("state"):
            parts.append(record["state"].lower())

        if record.get("full_address"):
            # Extract just the key parts
            addr = record["full_address"].lower()
            addr = re.sub(r'[^\w\s]', '', addr)
            parts.append(addr)

        return ' '.join(parts) if parts else ""

    def _string_similarity(self, s1: str, s2: str) -> float:
        """Calculate string similarity using multiple methods."""
        if not s1 or not s2:
            return 0.0

        if s1 == s2:
            return 1.0

        # Try rapidfuzz if available
        try:
            from rapidfuzz import fuzz
            return fuzz.ratio(s1, s2) / 100.0
        except ImportError:
            pass

        # Fallback: Jaccard similarity on words
        words1 = set(s1.split())
        words2 = set(s2.split())

        if not words1 or not words2:
            return 0.0

        intersection = len(words1 & words2)
        union = len(words1 | words2)

        return intersection / union if union > 0 else 0.0

    def _merge_group(
        self,
        records: list[dict],
        strategy: str
    ) -> tuple[dict, list[str]]:
        """Merge a group of matching records."""
        if not records:
            return {}, []

        if len(records) == 1:
            return records[0], []

        # Sort by quality score or completeness
        scored_records = []
        for record in records:
            score = record.get("quality_score", 0) or self._calculate_completeness(record)
            scored_records.append((score, record))

        scored_records.sort(key=lambda x: x[0], reverse=True)

        # Use highest quality as base
        canonical = scored_records[0][1].copy()
        aliases = []

        # Collect aliases
        for _, record in scored_records[1:]:
            name = record.get("company_name")
            if name and name != canonical.get("company_name"):
                aliases.append(name)

        # Merge data from other records
        if strategy == "merge_all":
            for _, record in scored_records[1:]:
                self._merge_record_into(canonical, record)

        # Update metadata
        canonical["merged_at"] = datetime.now(UTC).isoformat()
        canonical["merged_from_count"] = len(records)
        canonical["aliases"] = aliases

        # Merge provenance
        all_provenance = []
        for _, record in scored_records:
            if record.get("provenance"):
                if isinstance(record["provenance"], list):
                    all_provenance.extend(record["provenance"])
                else:
                    all_provenance.append(record["provenance"])

        if all_provenance:
            canonical["provenance"] = all_provenance

        # Merge associations
        all_associations = set()
        for _, record in scored_records:
            assocs = record.get("associations", [])
            if isinstance(assocs, list):
                all_associations.update(assocs)
            assoc = record.get("association")
            if assoc:
                all_associations.add(assoc)

        if all_associations:
            canonical["associations"] = sorted(list(all_associations))

        return canonical, aliases

    def _merge_record_into(self, target: dict, source: dict):
        """Merge source record fields into target."""
        for key, value in source.items():
            if key.startswith("_") or key in ["id", "created_at"]:
                continue

            existing = target.get(key)

            # Add missing fields
            if not existing and value:
                target[key] = value
                continue

            # Merge lists
            if isinstance(existing, list) and isinstance(value, list):
                combined = list(set(existing + value))
                target[key] = combined
                continue

            # Keep higher numeric values
            if key in ["employee_count_min", "employee_count_max", "revenue_min_usd", "quality_score"]:
                if isinstance(value, (int, float)) and isinstance(existing, (int, float)):
                    target[key] = max(value, existing)

    def _calculate_completeness(self, record: dict) -> int:
        """Calculate completeness score for a record."""
        fields = [
            "company_name", "website", "domain", "city", "state",
            "employee_count_min", "revenue_min_usd", "industry",
            "erp_system", "contacts"
        ]

        filled = sum(1 for f in fields if record.get(f))
        return int((filled / len(fields)) * 100)
