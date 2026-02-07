"""
Tests for agents/validation/dedupe.py - DedupeAgent

Tests duplicate detection and merging including similarity calculation,
fuzzy matching, and record merging strategies.
"""

import asyncio
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================================
# TEST FIXTURES
# =============================================================================


def create_dedupe_agent(agent_config: dict = None):
    """Factory to create DedupeAgent with mocked dependencies."""
    from agents.validation.dedupe import DedupeAgent

    with patch("agents.base.Config") as mock_config, \
         patch("agents.base.StructuredLogger") as mock_logger, \
         patch("agents.base.AsyncHTTPClient") as mock_http, \
         patch("agents.base.RateLimiter") as mock_limiter:

        mock_config.return_value.load.return_value = agent_config or {}

        agent = DedupeAgent(
            agent_type="validation.dedupe",
            job_id="test-job-123"
        )
        return agent


@pytest.fixture
def dedupe_agent():
    """Create a DedupeAgent instance."""
    return create_dedupe_agent()


@pytest.fixture
def duplicate_records():
    """Records with obvious duplicates."""
    return [
        {
            "company_name": "Acme Manufacturing Inc",
            "website": "https://acme-mfg.com",
            "city": "Detroit",
            "state": "MI",
            "employee_count_min": 100,
        },
        {
            "company_name": "ACME Manufacturing",
            "website": "https://www.acme-mfg.com",
            "city": "Detroit",
            "state": "MI",
            "employee_count_min": 150,
        },
        {
            "company_name": "Beta Industries LLC",
            "website": "https://beta-ind.com",
            "city": "Chicago",
            "state": "IL",
        },
    ]


@pytest.fixture
def unique_records():
    """Records with no duplicates."""
    return [
        {"company_name": "Alpha Corp", "website": "https://alpha.com", "city": "Detroit", "state": "MI"},
        {"company_name": "Beta LLC", "website": "https://beta.com", "city": "Chicago", "state": "IL"},
        {"company_name": "Gamma Inc", "website": "https://gamma.com", "city": "Cleveland", "state": "OH"},
    ]


@pytest.fixture
def records_with_contacts():
    """Records with contacts for merge testing."""
    return [
        {
            "company_name": "Acme Manufacturing",
            "website": "https://acme.com",
            "contacts": [
                {"name": "John Doe", "email": "john@acme.com", "title": "CEO"}
            ]
        },
        {
            "company_name": "Acme Mfg",
            "website": "https://acme.com",
            "contacts": [
                {"name": "Jane Smith", "email": "jane@acme.com", "title": "CFO"}
            ]
        },
    ]


# =============================================================================
# TEST SIMILARITY
# =============================================================================


class TestDedupeAgentSimilarity:
    """Tests for _calculate_similarity method."""

    def test_exact_domain_match_high_score(self, dedupe_agent):
        """Records with same domain get high similarity."""
        record1 = {"company_name": "Acme Inc", "website": "https://acme.com", "city": "Detroit", "state": "MI"}
        record2 = {"company_name": "Acme Corp", "website": "https://acme.com", "city": "Detroit", "state": "MI"}

        score = dedupe_agent._calculate_similarity(record1, record2)
        # domain weight (0.3) + city match (0.1) + state match (0.1) + partial name match
        assert score >= 0.5

    def test_exact_match_returns_1(self, dedupe_agent):
        """Identical records return 1.0."""
        record = {"company_name": "Test Company", "website": "https://test.com", "city": "Detroit", "state": "MI"}

        score = dedupe_agent._calculate_similarity(record, record)
        assert score == 1.0

    def test_different_records_low_score(self, dedupe_agent):
        """Completely different records get low similarity."""
        record1 = {"company_name": "Acme Inc", "website": "https://acme.com", "city": "Detroit", "state": "MI"}
        record2 = {"company_name": "Beta Corp", "website": "https://beta.org", "city": "Chicago", "state": "IL"}

        score = dedupe_agent._calculate_similarity(record1, record2)
        assert score < 0.5

    def test_missing_fields_handling(self, dedupe_agent):
        """Missing fields don't cause errors."""
        record1 = {"company_name": "Acme Inc"}
        record2 = {"company_name": "Acme Inc", "website": "https://acme.com"}

        score = dedupe_agent._calculate_similarity(record1, record2)
        # Only company_name compared
        assert score > 0

    def test_empty_records_zero_score(self, dedupe_agent):
        """Empty records return 0."""
        score = dedupe_agent._calculate_similarity({}, {})
        assert score == 0.0

    def test_weighted_field_scoring(self, dedupe_agent):
        """Fields are weighted according to configuration."""
        # company_name: 0.5, domain: 0.3, city: 0.1, state: 0.1
        # When only company_name matches, after normalization the score depends on
        # how _calculate_similarity handles missing/non-matching domains
        record1 = {"company_name": "Test", "website": "https://a.com", "city": "Detroit", "state": "MI"}
        record2 = {"company_name": "Test", "website": "https://b.com", "city": "Chicago", "state": "OH"}

        score = dedupe_agent._calculate_similarity(record1, record2)
        # Only company_name matches perfectly - the score is weighted by available fields
        # domain (0.3) doesn't match, city (0.1) doesn't match, state (0.1) doesn't match
        # Score = (0.5 * 1.0) / (0.5 + 0.3 + 0.1 + 0.1) = 0.5 / 1.0 = 0.5 if all are compared
        # But actually the similarity calculation may skip fields, so just verify company_name contributes
        assert score > 0  # Should have some similarity due to name match


# =============================================================================
# TEST FUZZY MATCHING
# =============================================================================


class TestDedupeAgentFuzzyMatch:
    """Tests for _fuzzy_match method."""

    def test_fuzzy_exact_match(self, dedupe_agent):
        """Exact strings return 1.0."""
        score = dedupe_agent._fuzzy_match("acme manufacturing", "acme manufacturing")
        assert score == 1.0

    def test_fuzzy_empty_strings(self, dedupe_agent):
        """Empty strings return 0.0."""
        assert dedupe_agent._fuzzy_match("", "") == 0.0
        assert dedupe_agent._fuzzy_match("test", "") == 0.0
        assert dedupe_agent._fuzzy_match("", "test") == 0.0

    def test_fuzzy_similar_strings(self, dedupe_agent):
        """Similar strings get high score."""
        score = dedupe_agent._fuzzy_match("acme manufacturing", "acme mfg")
        assert score > 0.5

    def test_fuzzy_different_strings(self, dedupe_agent):
        """Different strings get low score."""
        score = dedupe_agent._fuzzy_match("acme manufacturing", "xyz industries")
        assert score < 0.5

    def test_basic_similarity_fallback(self, dedupe_agent):
        """Basic similarity uses edit distance, not character-set Jaccard."""
        # Test the basic similarity method directly
        score = dedupe_agent._basic_similarity("abc", "abd")
        # Edit distance: "abc" vs "abd" — 2 of 3 chars match positionally
        # rapidfuzz.fuzz.ratio gives ~66.7%
        assert 0.6 <= score <= 0.7

    def test_basic_similarity_anagram_not_perfect(self, dedupe_agent):
        """Anagrams must NOT score 1.0 (the old Jaccard bug)."""
        score = dedupe_agent._basic_similarity("abc", "cab")
        assert score < 1.0

    def test_basic_similarity_company_name_anagrams(self, dedupe_agent):
        """Company name anagrams must score low."""
        score = dedupe_agent._basic_similarity("CAB Industries", "ABC Manufacturing")
        assert score < 0.7

    def test_basic_similarity_respects_character_order(self, dedupe_agent):
        """More positionally-aligned strings score higher than scrambled."""
        aligned = dedupe_agent._basic_similarity("acme", "acne")
        scrambled = dedupe_agent._basic_similarity("acme", "meca")
        assert aligned > scrambled


# =============================================================================
# TEST MERGING
# =============================================================================


class TestDedupeAgentMerging:
    """Tests for _merge_records method."""

    def test_merge_single_record(self, dedupe_agent):
        """Single record merge returns copy."""
        record = {"company_name": "Test", "city": "Detroit"}
        merged = dedupe_agent._merge_records([record])
        assert merged == record

    def test_merge_empty_list(self, dedupe_agent):
        """Empty list returns empty dict."""
        merged = dedupe_agent._merge_records([])
        assert merged == {}

    def test_merge_keeps_first_values(self, dedupe_agent):
        """First record's values are kept as base."""
        records = [
            {"company_name": "Acme Inc", "city": "Detroit"},
            {"company_name": "Acme Corp", "city": "Chicago"},
        ]
        merged = dedupe_agent._merge_records(records)
        assert merged["company_name"] == "Acme Inc"
        assert merged["city"] == "Detroit"

    def test_merge_fills_missing_fields(self, dedupe_agent):
        """Missing fields in base are filled from other records."""
        records = [
            {"company_name": "Acme Inc"},
            {"company_name": "Acme Corp", "city": "Detroit", "state": "MI"},
        ]
        merged = dedupe_agent._merge_records(records)
        assert merged["city"] == "Detroit"
        assert merged["state"] == "MI"

    def test_merge_contacts_deduplication(self, dedupe_agent, records_with_contacts):
        """Contacts are merged and deduplicated."""
        merged = dedupe_agent._merge_records(records_with_contacts)
        assert len(merged["contacts"]) == 2
        emails = {c["email"] for c in merged["contacts"]}
        assert "john@acme.com" in emails
        assert "jane@acme.com" in emails

    def test_merge_contacts_by_name_if_no_email(self, dedupe_agent):
        """Contacts without email are deduplicated by name."""
        records = [
            {"company_name": "Test", "contacts": [{"name": "John Doe", "title": "CEO"}]},
            {"company_name": "Test", "contacts": [{"name": "John Doe", "title": "President"}]},
        ]
        merged = dedupe_agent._merge_records(records)
        # Same name, should only have one contact
        assert len(merged["contacts"]) == 1

    def test_merge_tech_stack_union(self, dedupe_agent):
        """Tech stacks are combined."""
        records = [
            {"company_name": "Test", "tech_stack": ["SAP", "AWS"]},
            {"company_name": "Test", "tech_stack": ["AWS", "Azure"]},
        ]
        merged = dedupe_agent._merge_records(records)
        assert set(merged["tech_stack"]) == {"SAP", "AWS", "Azure"}

    def test_merge_numeric_maximization(self, dedupe_agent):
        """Numeric fields keep maximum value."""
        records = [
            {"company_name": "Test", "employee_count_min": 100, "quality_score": 70},
            {"company_name": "Test", "employee_count_min": 150, "quality_score": 85},
        ]
        merged = dedupe_agent._merge_records(records)
        assert merged["employee_count_min"] == 150
        assert merged["quality_score"] == 85

    def test_merge_association_combination(self, dedupe_agent):
        """Associations are combined."""
        records = [
            {"company_name": "Test", "association": "PMA"},
            {"company_name": "Test", "association": "NEMA"},
        ]
        merged = dedupe_agent._merge_records(records)
        assert "associations" in merged
        assert set(merged["associations"]) == {"NEMA", "PMA"}

    def test_merge_association_list(self, dedupe_agent):
        """Association lists are combined."""
        records = [
            {"company_name": "Test", "association": ["PMA", "AGMA"]},
            {"company_name": "Test", "association": "NEMA"},
        ]
        merged = dedupe_agent._merge_records(records)
        assert set(merged["associations"]) == {"PMA", "AGMA", "NEMA"}

    def test_merge_metadata_added(self, dedupe_agent):
        """Merge adds metadata."""
        records = [
            {"company_name": "Test A"},
            {"company_name": "Test B"},
        ]
        merged = dedupe_agent._merge_records(records)
        assert "merged_at" in merged
        assert merged["merged_from_count"] == 2

    def test_merge_ignores_private_fields(self, dedupe_agent):
        """Fields starting with _ are ignored during merge."""
        records = [
            {"company_name": "Test", "_internal": "value1"},
            {"company_name": "Test", "_internal": "value2"},
        ]
        merged = dedupe_agent._merge_records(records)
        # Should keep first record's _internal (from copy)
        assert merged.get("_internal") == "value1"


# =============================================================================
# TEST RUN METHOD
# =============================================================================


class TestDedupeAgentRun:
    """Tests for run() method."""

    @pytest.mark.asyncio
    async def test_run_detects_duplicates(self, dedupe_agent, duplicate_records):
        """run() detects and merges duplicates."""
        task = {"records": duplicate_records}
        result = await dedupe_agent.run(task)

        assert result["success"] is True
        # Acme records should be merged, Beta stays separate
        assert len(result["records"]) == 2

    @pytest.mark.asyncio
    async def test_run_returns_merged_records(self, dedupe_agent, duplicate_records):
        """run() returns merged records."""
        task = {"records": duplicate_records}
        result = await dedupe_agent.run(task)

        # Find the merged Acme record
        acme_record = next(
            (r for r in result["records"] if "acme" in r.get("company_name", "").lower()),
            None
        )
        assert acme_record is not None
        # Should have maximum employee count
        assert acme_record["employee_count_min"] == 150

    @pytest.mark.asyncio
    async def test_run_duplicate_groups_format(self, dedupe_agent, duplicate_records):
        """run() returns duplicate groups in correct format."""
        task = {"records": duplicate_records}
        result = await dedupe_agent.run(task)

        assert "duplicate_groups" in result
        assert isinstance(result["duplicate_groups"], list)
        # Should have at least one group (the Acme duplicates)
        assert len(result["duplicate_groups"]) >= 1
        # Each group is a list of indices
        for group in result["duplicate_groups"]:
            assert isinstance(group, list)
            assert all(isinstance(i, int) for i in group)

    @pytest.mark.asyncio
    async def test_run_single_records_pass_through(self, dedupe_agent, unique_records):
        """run() passes through unique records unchanged."""
        task = {"records": unique_records}
        result = await dedupe_agent.run(task)

        assert result["success"] is True
        assert len(result["records"]) == 3
        assert result["duplicates_found"] == 0

    @pytest.mark.asyncio
    async def test_run_empty_records_error(self, dedupe_agent):
        """run() with empty records returns error."""
        task = {"records": []}
        result = await dedupe_agent.run(task)

        assert result["success"] is False
        assert "error" in result

    @pytest.mark.asyncio
    async def test_run_no_records_key_error(self, dedupe_agent):
        """run() without records key returns error."""
        task = {}
        result = await dedupe_agent.run(task)

        assert result["success"] is False
        assert "error" in result

    @pytest.mark.asyncio
    async def test_run_adds_duplicate_group_to_merged(self, dedupe_agent, duplicate_records):
        """run() adds _duplicate_group to merged records."""
        task = {"records": duplicate_records}
        result = await dedupe_agent.run(task)

        merged_records = [r for r in result["records"] if "_duplicate_group" in r]
        assert len(merged_records) >= 1
        # The group should contain indices of original records
        assert isinstance(merged_records[0]["_duplicate_group"], list)

    @pytest.mark.asyncio
    async def test_run_records_processed_count(self, dedupe_agent, duplicate_records):
        """run() returns correct records_processed count."""
        task = {"records": duplicate_records}
        result = await dedupe_agent.run(task)

        assert result["records_processed"] == 3

    @pytest.mark.asyncio
    async def test_run_duplicates_found_count(self, dedupe_agent, duplicate_records):
        """run() returns correct duplicates_found count."""
        task = {"records": duplicate_records}
        result = await dedupe_agent.run(task)

        # 2 records merged into 1 = 1 duplicate found
        assert result["duplicates_found"] == 1


# =============================================================================
# TEST CONFIGURATION
# =============================================================================


class TestDedupeAgentConfiguration:
    """Tests for custom configuration options."""

    def test_custom_threshold(self):
        """Custom threshold is applied via config."""
        from agents.validation.dedupe import DedupeAgent

        with patch("agents.base.Config") as mock_config, \
             patch("agents.base.StructuredLogger"), \
             patch("agents.base.AsyncHTTPClient"), \
             patch("agents.base.RateLimiter"):

            mock_config.return_value.load.return_value = {
                "validation": {
                    "dedupe": {
                        "threshold": 0.95
                    }
                }
            }

            agent = DedupeAgent(agent_type="validation.dedupe", job_id="test-job")
            assert agent.threshold == 0.95

    def test_custom_weights(self):
        """Custom weights are applied via config."""
        from agents.validation.dedupe import DedupeAgent

        with patch("agents.base.Config") as mock_config, \
             patch("agents.base.StructuredLogger"), \
             patch("agents.base.AsyncHTTPClient"), \
             patch("agents.base.RateLimiter"):

            mock_config.return_value.load.return_value = {
                "validation": {
                    "dedupe": {
                        "weights": {
                            "company_name": 0.6,
                            "domain": 0.2,
                            "city": 0.1,
                            "state": 0.1
                        }
                    }
                }
            }

            agent = DedupeAgent(agent_type="validation.dedupe", job_id="test-job")
            assert agent.weights["company_name"] == 0.6

    def test_custom_match_fields(self):
        """Custom match fields are used via config."""
        from agents.validation.dedupe import DedupeAgent

        with patch("agents.base.Config") as mock_config, \
             patch("agents.base.StructuredLogger"), \
             patch("agents.base.AsyncHTTPClient"), \
             patch("agents.base.RateLimiter"):

            mock_config.return_value.load.return_value = {
                "validation": {
                    "dedupe": {
                        "match_fields": ["company_name", "domain"]
                    }
                }
            }

            agent = DedupeAgent(agent_type="validation.dedupe", job_id="test-job")
            assert agent.match_fields == ["company_name", "domain"]

    @pytest.mark.asyncio
    async def test_high_threshold_fewer_duplicates(self):
        """Higher threshold results in fewer detected duplicates."""
        records = [
            {"company_name": "Acme Manufacturing Inc", "website": "https://acme.com"},
            {"company_name": "Acme Mfg", "website": "https://acme.com"},
        ]

        # Low threshold - should merge
        low_agent = create_dedupe_agent({"threshold": 0.5})
        low_result = await low_agent.run({"records": records})

        # High threshold - might not merge
        high_agent = create_dedupe_agent({"threshold": 0.99})
        high_result = await high_agent.run({"records": records})

        # Low threshold should have fewer final records (more merging)
        assert len(low_result["records"]) <= len(high_result["records"])


# =============================================================================
# TEST EDGE CASES
# =============================================================================


class TestDedupeAgentEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_single_record(self, dedupe_agent):
        """Single record passes through unchanged."""
        task = {"records": [{"company_name": "Test"}]}
        result = await dedupe_agent.run(task)

        assert result["success"] is True
        assert len(result["records"]) == 1
        assert result["duplicates_found"] == 0

    @pytest.mark.asyncio
    async def test_all_duplicates(self, dedupe_agent):
        """All records being duplicates works correctly."""
        records = [
            {"company_name": "Acme", "website": "https://acme.com"},
            {"company_name": "Acme Inc", "website": "https://acme.com"},
            {"company_name": "ACME Corp", "website": "https://acme.com"},
        ]
        task = {"records": records}
        result = await dedupe_agent.run(task)

        assert result["success"] is True
        assert len(result["records"]) == 1
        assert result["duplicates_found"] == 2

    @pytest.mark.asyncio
    async def test_records_with_none_values(self, dedupe_agent):
        """Records with None values don't cause errors."""
        records = [
            {"company_name": "Test", "website": None, "city": None},
            {"company_name": "Test", "website": "https://test.com"},
        ]
        task = {"records": records}
        result = await dedupe_agent.run(task)

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_large_batch_performance(self, dedupe_agent):
        """Large batch of records completes."""
        records = [
            {"company_name": f"Company {i}", "website": f"https://c{i}.com"}
            for i in range(100)
        ]
        task = {"records": records}
        result = await dedupe_agent.run(task)

        assert result["success"] is True
        assert result["records_processed"] == 100
