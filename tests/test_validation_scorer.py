"""
Tests for agents/validation/scorer.py - ScorerAgent

Tests quality scoring calculation including completeness, accuracy,
freshness, source reliability, and final grading.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

# =============================================================================
# TEST FIXTURES
# =============================================================================


def create_scorer_agent(agent_config: dict = None):
    """Factory to create ScorerAgent with mocked dependencies."""
    from agents.validation.scorer import ScorerAgent

    with patch("agents.base.Config") as mock_config, \
         patch("agents.base.StructuredLogger"), \
         patch("agents.base.AsyncHTTPClient"), \
         patch("agents.base.RateLimiter"):

        mock_config.return_value.load.return_value = agent_config or {}

        agent = ScorerAgent(
            agent_type="validation.scorer",
            job_id="test-job-123"
        )
        return agent


@pytest.fixture
def scorer_agent():
    """Create a ScorerAgent instance."""
    return create_scorer_agent()


@pytest.fixture
def complete_record():
    """A record with all required and valuable fields."""
    return {
        "company_name": "Acme Manufacturing Inc",
        "website": "https://acme-mfg.com",
        "city": "Detroit",
        "state": "MI",
        "employee_count_min": 100,
        "revenue_min_usd": 10000000,
        "erp_system": "SAP",
        "contacts": [{"name": "John Doe", "title": "CEO"}],
        "year_founded": 1985,
        "naics_code": "332710",
        "industry": "Manufacturing",
        "phone": "+1-555-123-4567",
        "email": "info@acme-mfg.com",
        "extracted_at": datetime.now(UTC).isoformat(),
        "firmographic_source": "clearbit",
        "association": "PMA"
    }


@pytest.fixture
def minimal_record():
    """A record with only minimal fields."""
    return {
        "company_name": "Test Company"
    }


@pytest.fixture
def validated_record():
    """A record with validation results."""
    return {
        "company_name": "Acme Manufacturing Inc",
        "website": "https://acme-mfg.com",
        "city": "Detroit",
        "state": "MI",
        "_validation": {
            "dns_mx_valid": True,
            "google_places_matched": True,
            "linkedin_found": True
        },
        "extracted_at": datetime.now(UTC).isoformat()
    }


# =============================================================================
# TEST COMPLETENESS
# =============================================================================


class TestScorerAgentCompleteness:
    """Tests for _calculate_completeness method."""

    def test_completeness_all_required_fields_present(self, scorer_agent, complete_record):
        """Record with all required fields gets full required points."""
        score = scorer_agent._calculate_completeness(complete_record)
        # 4/4 required = 60 points base
        # Plus valuable fields
        assert score >= 60  # At least full required points

    def test_completeness_missing_required_fields(self, scorer_agent):
        """Record missing required fields gets lower score."""
        record = {
            "company_name": "Test",
            # Missing website, city, state
        }
        score = scorer_agent._calculate_completeness(record)
        # 1/4 required = 15 points
        assert score < 30

    def test_completeness_with_valuable_fields(self, scorer_agent):
        """Valuable fields add to completeness score."""
        base_record = {
            "company_name": "Test",
            "website": "https://test.com",
            "city": "Detroit",
            "state": "MI",
        }
        base_score = scorer_agent._calculate_completeness(base_record)

        enriched_record = base_record.copy()
        enriched_record["employee_count_min"] = 100
        enriched_record["erp_system"] = "SAP"
        enriched_score = scorer_agent._calculate_completeness(enriched_record)

        assert enriched_score > base_score

    def test_completeness_empty_record(self, scorer_agent):
        """Empty record gets zero completeness."""
        score = scorer_agent._calculate_completeness({})
        assert score == 0

    def test_completeness_empty_string_not_counted(self, scorer_agent):
        """Empty string values are not counted as filled."""
        record = {
            "company_name": "Test",
            "website": "",  # Empty string
            "city": "   ",  # Whitespace only
            "state": None,  # None
        }
        score = scorer_agent._calculate_completeness(record)
        # Only company_name counts
        assert score == 15  # 1/4 * 60

    def test_completeness_empty_list_not_counted(self, scorer_agent):
        """Empty list values are not counted as filled."""
        record = {
            "company_name": "Test",
            "website": "https://test.com",
            "city": "Detroit",
            "state": "MI",
            "contacts": [],  # Empty list
        }
        score = scorer_agent._calculate_completeness(record)
        # contacts (valuable) shouldn't be counted
        expected_base = 60  # All required
        assert score == expected_base


# =============================================================================
# TEST ACCURACY
# =============================================================================


class TestScorerAgentAccuracy:
    """Tests for _calculate_accuracy method."""

    def test_accuracy_no_validation_returns_60(self, scorer_agent):
        """Record without validation gets moderate default score."""
        record = {"company_name": "Test"}
        score = scorer_agent._calculate_accuracy(record)
        assert score == 60

    def test_accuracy_dns_valid_bonus(self, scorer_agent):
        """Valid DNS adds 20 points."""
        record = {
            "_validation": {"dns_mx_valid": True}
        }
        score = scorer_agent._calculate_accuracy(record)
        assert score == 80  # 60 base + 20 DNS

    def test_accuracy_dns_invalid_penalty(self, scorer_agent):
        """Invalid DNS subtracts 20 points."""
        record = {
            "_validation": {"dns_mx_valid": False}
        }
        score = scorer_agent._calculate_accuracy(record)
        assert score == 40  # 60 base - 20 DNS

    def test_accuracy_places_match_bonus(self, scorer_agent):
        """Google Places match adds 15 points."""
        record = {
            "_validation": {"google_places_matched": True}
        }
        score = scorer_agent._calculate_accuracy(record)
        assert score == 75  # 60 base + 15 places

    def test_accuracy_places_mismatch_penalty(self, scorer_agent):
        """Google Places mismatch subtracts 10 points."""
        record = {
            "_validation": {"google_places_matched": False}
        }
        score = scorer_agent._calculate_accuracy(record)
        assert score == 50  # 60 base - 10 places

    def test_accuracy_linkedin_bonus(self, scorer_agent):
        """LinkedIn found adds 5 points."""
        record = {
            "_validation": {"linkedin_found": True}
        }
        score = scorer_agent._calculate_accuracy(record)
        assert score == 65  # 60 base + 5 linkedin

    def test_accuracy_combined_validation(self, scorer_agent, validated_record):
        """All positive validations combine correctly."""
        score = scorer_agent._calculate_accuracy(validated_record)
        # 60 + 20 (DNS) + 15 (places) + 5 (linkedin) = 100
        assert score == 100

    def test_accuracy_blends_validation_score(self, scorer_agent):
        """External validation_score is blended with computed score."""
        record = {
            "_validation": {"dns_mx_valid": True},  # 60 + 20 = 80
            "validation_score": 60  # External score
        }
        score = scorer_agent._calculate_accuracy(record)
        # (80 + 60) / 2 = 70
        assert score == 70

    def test_accuracy_clamped_to_0_100(self, scorer_agent):
        """Accuracy score is clamped between 0 and 100."""
        record = {
            "_validation": {
                "dns_mx_valid": False,
                "google_places_matched": False
            }
        }
        score = scorer_agent._calculate_accuracy(record)
        # 60 - 20 - 10 = 30, should not go below 0
        assert score >= 0
        assert score <= 100


# =============================================================================
# TEST FRESHNESS
# =============================================================================


class TestScorerAgentFreshness:
    """Tests for _calculate_freshness method."""

    def test_freshness_within_7_days(self, scorer_agent):
        """Data within 7 days gets 100."""
        record = {"extracted_at": datetime.now(UTC).isoformat()}
        score = scorer_agent._calculate_freshness(record)
        assert score == 100

    def test_freshness_30_days(self, scorer_agent):
        """Data 15 days old gets 90."""
        dt = datetime.now(UTC) - timedelta(days=15)
        record = {"extracted_at": dt.isoformat()}
        score = scorer_agent._calculate_freshness(record)
        assert score == 90

    def test_freshness_90_days(self, scorer_agent):
        """Data 60 days old gets 75."""
        dt = datetime.now(UTC) - timedelta(days=60)
        record = {"extracted_at": dt.isoformat()}
        score = scorer_agent._calculate_freshness(record)
        assert score == 75

    def test_freshness_180_days(self, scorer_agent):
        """Data 120 days old gets 60."""
        dt = datetime.now(UTC) - timedelta(days=120)
        record = {"extracted_at": dt.isoformat()}
        score = scorer_agent._calculate_freshness(record)
        assert score == 60

    def test_freshness_365_days(self, scorer_agent):
        """Data 200 days old gets 40."""
        dt = datetime.now(UTC) - timedelta(days=200)
        record = {"extracted_at": dt.isoformat()}
        score = scorer_agent._calculate_freshness(record)
        assert score == 40

    def test_freshness_over_year(self, scorer_agent):
        """Data over a year old gets 20."""
        dt = datetime.now(UTC) - timedelta(days=400)
        record = {"extracted_at": dt.isoformat()}
        score = scorer_agent._calculate_freshness(record)
        assert score == 20

    def test_freshness_missing_timestamp(self, scorer_agent):
        """Missing extracted_at gets default 50."""
        record = {"company_name": "Test"}
        score = scorer_agent._calculate_freshness(record)
        assert score == 50

    def test_freshness_handles_z_suffix(self, scorer_agent):
        """Handles Z suffix in ISO timestamp."""
        dt = datetime.now(UTC)
        record = {"extracted_at": dt.strftime("%Y-%m-%dT%H:%M:%SZ")}
        score = scorer_agent._calculate_freshness(record)
        assert score == 100

    def test_freshness_handles_invalid_timestamp(self, scorer_agent):
        """Invalid timestamp returns default 50."""
        record = {"extracted_at": "not-a-date"}
        score = scorer_agent._calculate_freshness(record)
        assert score == 50


# =============================================================================
# TEST SOURCE RELIABILITY
# =============================================================================


class TestScorerAgentSourceReliability:
    """Tests for _calculate_source_reliability method."""

    def test_source_reliability_clearbit(self, scorer_agent):
        """Clearbit source gets high score."""
        record = {"firmographic_source": "clearbit"}
        score = scorer_agent._calculate_source_reliability(record)
        # clearbit = 95 + 2 (one source bonus)
        assert score >= 95

    def test_source_reliability_unknown_source(self, scorer_agent):
        """Unknown source gets moderate score."""
        record = {"firmographic_source": "random_source"}
        score = scorer_agent._calculate_source_reliability(record)
        # unknown default = 50 + 2 (one source bonus)
        assert score >= 50

    def test_source_reliability_no_sources(self, scorer_agent):
        """No sources gets default 50."""
        record = {"company_name": "Test"}
        score = scorer_agent._calculate_source_reliability(record)
        assert score == 50

    def test_source_reliability_multiple_sources_bonus(self, scorer_agent):
        """Multiple sources get bonus."""
        record = {
            "firmographic_source": "clearbit",
            "tech_source": "builtwith",
            "association": "PMA"
        }
        score = scorer_agent._calculate_source_reliability(record)
        # Should have multiple source bonus
        assert score > 80

    def test_source_reliability_contact_sources(self, scorer_agent):
        """Contact sources contribute to score."""
        record = {
            "contacts": [
                {"name": "John", "source": "apollo"},
                {"name": "Jane", "source": "zoominfo"}
            ]
        }
        score = scorer_agent._calculate_source_reliability(record)
        # apollo=85, zoominfo=90, plus bonuses
        assert score > 85


# =============================================================================
# TEST GRADING
# =============================================================================


class TestScorerAgentGrading:
    """Tests for _get_grade method."""

    def test_grade_a_boundary_90(self, scorer_agent):
        """Score of 90 gets grade A."""
        grade = scorer_agent._get_grade(90)
        assert grade == "A"

    def test_grade_a_boundary_100(self, scorer_agent):
        """Score of 100 gets grade A."""
        grade = scorer_agent._get_grade(100)
        assert grade == "A"

    def test_grade_b_boundary_80(self, scorer_agent):
        """Score of 80 gets grade B."""
        grade = scorer_agent._get_grade(80)
        assert grade == "B"

    def test_grade_b_boundary_89(self, scorer_agent):
        """Score of 89 gets grade B."""
        grade = scorer_agent._get_grade(89)
        assert grade == "B"

    def test_grade_c_boundary_70(self, scorer_agent):
        """Score of 70 gets grade C."""
        grade = scorer_agent._get_grade(70)
        assert grade == "C"

    def test_grade_c_boundary_79(self, scorer_agent):
        """Score of 79 gets grade C."""
        grade = scorer_agent._get_grade(79)
        assert grade == "C"

    def test_grade_d_boundary_60(self, scorer_agent):
        """Score of 60 gets grade D."""
        grade = scorer_agent._get_grade(60)
        assert grade == "D"

    def test_grade_d_boundary_69(self, scorer_agent):
        """Score of 69 gets grade D."""
        grade = scorer_agent._get_grade(69)
        assert grade == "D"

    def test_grade_f_boundary_59(self, scorer_agent):
        """Score of 59 gets grade F."""
        grade = scorer_agent._get_grade(59)
        assert grade == "F"

    def test_grade_f_boundary_0(self, scorer_agent):
        """Score of 0 gets grade F."""
        grade = scorer_agent._get_grade(0)
        assert grade == "F"


# =============================================================================
# TEST RUN METHOD
# =============================================================================


class TestScorerAgentRun:
    """Tests for run() method."""

    @pytest.mark.asyncio
    async def test_run_calculates_weighted_score(self, scorer_agent, complete_record):
        """run() calculates weighted final score."""
        task = {"records": [complete_record]}
        result = await scorer_agent.run(task)

        assert result["success"] is True
        assert len(result["records"]) == 1
        assert "quality_score" in result["records"][0]
        assert "quality_grade" in result["records"][0]
        assert 0 <= result["records"][0]["quality_score"] <= 100

    @pytest.mark.asyncio
    async def test_run_quality_distribution(self, scorer_agent):
        """run() returns quality distribution."""
        records = [
            {"company_name": "A", "website": "a.com", "city": "X", "state": "MI",
             "extracted_at": datetime.now(UTC).isoformat()},
            {"company_name": "B", "website": "b.com", "city": "Y", "state": "OH",
             "extracted_at": datetime.now(UTC).isoformat()},
        ]
        task = {"records": records}
        result = await scorer_agent.run(task)

        assert "quality_distribution" in result
        assert set(result["quality_distribution"].keys()) == {"A", "B", "C", "D", "F"}

    @pytest.mark.asyncio
    async def test_run_average_and_median(self, scorer_agent):
        """run() calculates average and median scores."""
        records = [
            {"company_name": f"Company {i}", "website": f"c{i}.com",
             "city": "Detroit", "state": "MI",
             "extracted_at": datetime.now(UTC).isoformat()}
            for i in range(5)
        ]
        task = {"records": records}
        result = await scorer_agent.run(task)

        assert "average_score" in result
        assert "median_score" in result
        assert isinstance(result["average_score"], float)
        assert isinstance(result["median_score"], int)

    @pytest.mark.asyncio
    async def test_run_empty_records_error(self, scorer_agent):
        """run() with empty records returns error."""
        task = {"records": []}
        result = await scorer_agent.run(task)

        assert result["success"] is False
        assert "error" in result

    @pytest.mark.asyncio
    async def test_run_no_records_key_error(self, scorer_agent):
        """run() without records key returns error."""
        task = {}
        result = await scorer_agent.run(task)

        assert result["success"] is False
        assert "error" in result

    @pytest.mark.asyncio
    async def test_run_adds_scoring_metadata(self, scorer_agent, minimal_record):
        """run() adds scoring metadata to records."""
        task = {"records": [minimal_record]}
        result = await scorer_agent.run(task)

        record = result["records"][0]
        assert "scored_at" in record
        assert "_quality_components" in record
        assert "completeness" in record["_quality_components"]
        assert "accuracy" in record["_quality_components"]
        assert "freshness" in record["_quality_components"]
        assert "source_reliability" in record["_quality_components"]

    @pytest.mark.asyncio
    async def test_run_records_processed_count(self, scorer_agent):
        """run() returns correct records_processed count."""
        records = [{"company_name": f"C{i}"} for i in range(10)]
        task = {"records": records}
        result = await scorer_agent.run(task)

        assert result["records_processed"] == 10


# =============================================================================
# TEST HAS_VALUE HELPER
# =============================================================================


class TestScorerAgentHasValue:
    """Tests for _has_value helper method."""

    def test_has_value_none(self, scorer_agent):
        """None is not a value."""
        assert scorer_agent._has_value(None) is False

    def test_has_value_empty_string(self, scorer_agent):
        """Empty string is not a value."""
        assert scorer_agent._has_value("") is False

    def test_has_value_whitespace_string(self, scorer_agent):
        """Whitespace-only string is not a value."""
        assert scorer_agent._has_value("   ") is False

    def test_has_value_empty_list(self, scorer_agent):
        """Empty list is not a value."""
        assert scorer_agent._has_value([]) is False

    def test_has_value_valid_string(self, scorer_agent):
        """Non-empty string is a value."""
        assert scorer_agent._has_value("test") is True

    def test_has_value_valid_list(self, scorer_agent):
        """Non-empty list is a value."""
        assert scorer_agent._has_value(["item"]) is True

    def test_has_value_zero(self, scorer_agent):
        """Zero is a value."""
        assert scorer_agent._has_value(0) is True

    def test_has_value_false(self, scorer_agent):
        """False is a value."""
        assert scorer_agent._has_value(False) is True


# =============================================================================
# TEST CUSTOM CONFIGURATION
# =============================================================================


class TestScorerAgentConfiguration:
    """Tests for custom configuration options."""

    def test_custom_weights(self):
        """Custom weights are applied via direct agent_config override."""
        from agents.validation.scorer import ScorerAgent

        with patch("agents.base.Config") as mock_config, \
             patch("agents.base.StructuredLogger"), \
             patch("agents.base.AsyncHTTPClient"), \
             patch("agents.base.RateLimiter"):

            # Config.load("agents") returns nested config under validation.scorer
            mock_config.return_value.load.return_value = {
                "validation": {
                    "scorer": {
                        "weights": {
                            "completeness": 0.50,
                            "accuracy": 0.30,
                            "freshness": 0.10,
                            "source_reliability": 0.10
                        }
                    }
                }
            }

            agent = ScorerAgent(agent_type="validation.scorer", job_id="test-job")
            assert agent.weights["completeness"] == 0.50
            assert agent.weights["accuracy"] == 0.30

    def test_custom_required_fields(self):
        """Custom required fields are used via config."""
        from agents.validation.scorer import ScorerAgent

        with patch("agents.base.Config") as mock_config, \
             patch("agents.base.StructuredLogger"), \
             patch("agents.base.AsyncHTTPClient"), \
             patch("agents.base.RateLimiter"):

            mock_config.return_value.load.return_value = {
                "validation": {
                    "scorer": {
                        "required_fields": ["company_name", "domain"]
                    }
                }
            }

            agent = ScorerAgent(agent_type="validation.scorer", job_id="test-job")
            assert agent.REQUIRED_FIELDS == ["company_name", "domain"]

    def test_custom_source_scores(self):
        """Custom source scores are used via config."""
        from agents.validation.scorer import ScorerAgent

        with patch("agents.base.Config") as mock_config, \
             patch("agents.base.StructuredLogger"), \
             patch("agents.base.AsyncHTTPClient"), \
             patch("agents.base.RateLimiter"):

            mock_config.return_value.load.return_value = {
                "validation": {
                    "scorer": {
                        "source_scores": {
                            "custom_source": 99,
                            "default": 30
                        }
                    }
                }
            }

            agent = ScorerAgent(agent_type="validation.scorer", job_id="test-job")
            assert agent.SOURCE_SCORES["custom_source"] == 99
