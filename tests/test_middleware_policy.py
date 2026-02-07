"""
Tests for middleware/policy.py - Policy Enforcement Middleware

Tests decorator-based policy enforcement: provenance, crawler-only,
enrichment HTTP, JSON validation, auth detection, ontology labels.
"""

from unittest.mock import MagicMock

import pytest

from middleware.policy import (
    ENRICHMENT_AGENTS,
    PolicyChecker,
    PolicyViolation,
    auth_pages_flagged,
    crawler_only,
    enforce_provenance,
    enrichment_http,
    is_crawler_agent,
    is_enrichment_agent,
    ontology_labels_required,
    validate_json_output,
)

# =============================================================================
# ENRICHMENT_AGENTS SET
# =============================================================================


class TestEnrichmentAgentsSet:
    """Tests for ENRICHMENT_AGENTS constant."""

    def test_contains_firmographic(self):
        assert "firmographic" in ENRICHMENT_AGENTS

    def test_contains_tech_stack(self):
        assert "tech_stack" in ENRICHMENT_AGENTS

    def test_contains_contact_finder(self):
        assert "contact_finder" in ENRICHMENT_AGENTS

    def test_does_not_contain_html_parser(self):
        assert "html_parser" not in ENRICHMENT_AGENTS

    def test_does_not_contain_dedupe(self):
        assert "dedupe" not in ENRICHMENT_AGENTS


# =============================================================================
# @enrichment_http DECORATOR
# =============================================================================


class TestEnrichmentHttpDecorator:
    """Tests for @enrichment_http decorator."""

    @pytest.mark.asyncio
    async def test_allows_firmographic_agent(self):
        """Authorized enrichment agent can call decorated method."""
        mock_self = MagicMock()
        mock_self.agent_type = "enrichment.firmographic"

        @enrichment_http
        async def fetch(self, url):
            return {"data": "result"}

        result = await fetch(mock_self, "https://api.clearbit.com/v2/companies")
        assert result == {"data": "result"}

    @pytest.mark.asyncio
    async def test_allows_tech_stack_agent(self):
        """tech_stack agent is authorized."""
        mock_self = MagicMock()
        mock_self.agent_type = "enrichment.tech_stack"

        @enrichment_http
        async def fetch(self, url):
            return {"ok": True}

        result = await fetch(mock_self, "https://api.builtwith.com")
        assert result["ok"] is True

    @pytest.mark.asyncio
    async def test_allows_contact_finder_agent(self):
        """contact_finder agent is authorized."""
        mock_self = MagicMock()
        mock_self.agent_type = "enrichment.contact_finder"

        @enrichment_http
        async def fetch(self, url):
            return {"contacts": []}

        result = await fetch(mock_self, "https://api.apollo.io")
        assert result == {"contacts": []}

    @pytest.mark.asyncio
    async def test_blocks_html_parser(self):
        """Non-enrichment agent is blocked."""
        mock_self = MagicMock()
        mock_self.agent_type = "extraction.html_parser"

        @enrichment_http
        async def fetch(self, url):
            return {"data": "should not reach"}

        with pytest.raises(PolicyViolation) as exc_info:
            await fetch(mock_self, "https://api.clearbit.com")

        assert "enrichment_http_only" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_blocks_link_crawler(self):
        """Crawler agent is blocked from enrichment HTTP."""
        mock_self = MagicMock()
        mock_self.agent_type = "discovery.link_crawler"

        @enrichment_http
        async def fetch(self, url):
            return {}

        with pytest.raises(PolicyViolation):
            await fetch(mock_self, "https://api.clearbit.com")

    @pytest.mark.asyncio
    async def test_blocks_unknown_agent(self):
        """Unknown agent type is blocked."""
        mock_self = MagicMock()
        mock_self.agent_type = "unknown"

        @enrichment_http
        async def fetch(self, url):
            return {}

        with pytest.raises(PolicyViolation):
            await fetch(mock_self, "https://api.clearbit.com")


# =============================================================================
# is_enrichment_agent() HELPER
# =============================================================================


class TestIsEnrichmentAgent:
    """Tests for is_enrichment_agent() helper."""

    def test_fully_qualified_firmographic(self):
        assert is_enrichment_agent("enrichment.firmographic") is True

    def test_short_name_tech_stack(self):
        assert is_enrichment_agent("tech_stack") is True

    def test_non_enrichment_agent(self):
        assert is_enrichment_agent("extraction.html_parser") is False

    def test_empty_string(self):
        assert is_enrichment_agent("") is False


# =============================================================================
# @crawler_only DECORATOR (existing behavior preserved)
# =============================================================================


class TestCrawlerOnlyDecorator:
    """Tests for @crawler_only decorator (existing behavior)."""

    @pytest.mark.asyncio
    async def test_allows_link_crawler(self):
        """Crawler agent can call decorated method."""
        mock_self = MagicMock()
        mock_self.agent_type = "discovery.link_crawler"

        @crawler_only
        async def fetch(self, url):
            return "<html>"

        result = await fetch(mock_self, "https://example.com")
        assert result == "<html>"

    @pytest.mark.asyncio
    async def test_blocks_enrichment_agent(self):
        """Enrichment agent is blocked from page fetching."""
        mock_self = MagicMock()
        mock_self.agent_type = "enrichment.firmographic"

        @crawler_only
        async def fetch(self, url):
            return "<html>"

        with pytest.raises(PolicyViolation) as exc_info:
            await fetch(mock_self, "https://example.com")
        assert "no_agent_fetches_pages_except_crawler" in str(exc_info.value)

    def test_is_crawler_agent_true(self):
        assert is_crawler_agent("discovery.link_crawler") is True

    def test_is_crawler_agent_false(self):
        assert is_crawler_agent("validation.dedupe") is False


# =============================================================================
# PolicyChecker
# =============================================================================


class TestPolicyChecker:
    """Tests for PolicyChecker static methods."""

    def test_check_enrichment_permission_true(self):
        assert PolicyChecker.check_enrichment_permission("enrichment.firmographic") is True

    def test_check_enrichment_permission_false(self):
        assert PolicyChecker.check_enrichment_permission("extraction.html_parser") is False

    def test_check_crawler_permission_true(self):
        assert PolicyChecker.check_crawler_permission("discovery.link_crawler") is True

    def test_check_crawler_permission_false(self):
        assert PolicyChecker.check_crawler_permission("enrichment.firmographic") is False

    def test_check_json_serializable_valid(self):
        ok, err = PolicyChecker.check_json_serializable({"key": "value"})
        assert ok is True
        assert err is None

    def test_check_provenance_missing(self):
        ok, missing = PolicyChecker.check_provenance({"company_name": "Acme"})
        assert ok is False
        assert "source_url" in missing

    def test_check_provenance_present(self):
        ok, missing = PolicyChecker.check_provenance({
            "source_url": "https://example.com",
            "extracted_at": "2024-01-01"
        })
        assert ok is True
        assert missing == []

    def test_check_auth_required_detected(self):
        found, indicator = PolicyChecker.check_auth_required("<p>Please log in to continue</p>")
        assert found is True
        assert indicator == "please log in"

    def test_check_auth_required_not_found(self):
        found, indicator = PolicyChecker.check_auth_required("<p>Welcome to our site</p>")
        assert found is False
        assert indicator is None


# =============================================================================
# @enforce_provenance
# =============================================================================


class TestEnforceProvenance:
    """Tests for @enforce_provenance decorator."""

    @pytest.mark.asyncio
    async def test_logs_warning_for_missing_provenance(self):
        """Logs warning when records lack provenance."""
        mock_self = MagicMock()
        mock_self.agent_type = "test.agent"
        mock_self.job_id = "test-123"

        @enforce_provenance
        async def run(self, task):
            return {"records": [{"company_name": "Acme"}]}

        result = await run(mock_self, {})
        # Should still return result (warning only)
        assert len(result["records"]) == 1


# =============================================================================
# @validate_json_output
# =============================================================================


class TestValidateJsonOutput:
    """Tests for @validate_json_output decorator."""

    @pytest.mark.asyncio
    async def test_valid_json_passes(self):
        """Valid JSON result passes."""
        mock_self = MagicMock()
        mock_self.agent_type = "test.agent"

        @validate_json_output
        async def run(self, task):
            return {"success": True, "records": []}

        result = await run(mock_self, {})
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_invalid_json_raises(self):
        """Non-serializable result raises PolicyViolation."""
        mock_self = MagicMock()
        mock_self.agent_type = "test.agent"

        @validate_json_output
        async def run(self, task):
            # default=str handles most objects, but circular refs still fail
            d = {}
            d["self"] = d
            return d

        with pytest.raises(PolicyViolation) as exc_info:
            await run(mock_self, {})
        assert "outputs_must_be_valid_json" in str(exc_info.value)


# =============================================================================
# @auth_pages_flagged
# =============================================================================


class TestAuthPagesFlagged:
    """Tests for @auth_pages_flagged decorator."""

    @pytest.mark.asyncio
    async def test_auth_page_detected(self):
        """Auth page returns flagged result."""
        mock_self = MagicMock()
        mock_self.agent_type = "test.agent"

        @auth_pages_flagged
        async def extract(self, html=None, url=None):
            return {"records": []}

        # Pass as keyword to use the kwargs path in the decorator
        result = await extract(
            mock_self,
            html="<p>Please log in to view this content</p>",
            url="https://example.com",
        )
        assert result["auth_required"] is True
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_normal_page_passes(self):
        """Normal page passes through."""
        mock_self = MagicMock()
        mock_self.agent_type = "test.agent"

        @auth_pages_flagged
        async def extract(self, html=None, url=None):
            return {"records": [{"name": "Acme"}]}

        result = await extract(mock_self, html="<p>Welcome to Acme Manufacturing</p>")
        assert result["records"][0]["name"] == "Acme"


# =============================================================================
# @ontology_labels_required
# =============================================================================


class TestOntologyLabelsRequired:
    """Tests for @ontology_labels_required decorator."""

    @pytest.mark.asyncio
    async def test_missing_labels_raises(self):
        """Missing required labels raises PolicyViolation."""
        mock_self = MagicMock()
        mock_self.agent_type = "test.agent"

        @ontology_labels_required("page_type", "entity_type")
        async def classify(self, page):
            return {"records": [{"page_type": "MEMBER_DIRECTORY"}]}

        with pytest.raises(PolicyViolation) as exc_info:
            await classify(mock_self, "<html>")
        assert "entity_type" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_all_labels_present_passes(self):
        """All required labels present passes."""
        mock_self = MagicMock()
        mock_self.agent_type = "test.agent"

        @ontology_labels_required("page_type")
        async def classify(self, page):
            return {"records": [{"page_type": "MEMBER_DIRECTORY"}]}

        result = await classify(mock_self, "<html>")
        assert result["records"][0]["page_type"] == "MEMBER_DIRECTORY"


# =============================================================================
# PolicyViolation
# =============================================================================


class TestPolicyViolation:
    """Tests for PolicyViolation exception."""

    def test_policy_violation_message(self):
        exc = PolicyViolation(
            policy="test_policy",
            message="Something went wrong",
            agent="test.agent"
        )
        assert "test_policy" in str(exc)
        assert "Something went wrong" in str(exc)
        assert exc.policy == "test_policy"
        assert exc.agent == "test.agent"

    def test_policy_violation_context(self):
        exc = PolicyViolation(
            policy="test_policy",
            message="Error",
            context={"key": "value"}
        )
        assert exc.context == {"key": "value"}
