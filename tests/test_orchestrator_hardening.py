"""
Tests for orchestrator error thresholds, metrics, and --log-level.

Phase 6: Orchestrator Hardening
"""

import asyncio
import json
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from click.testing import CliRunner


# =============================================================================
# HELPERS
# =============================================================================


def _make_orchestrator(
    associations_config=None,
    agent_config=None,
    mode="extract",
    associations=None,
    dry_run=True,
):
    """Create an OrchestratorAgent for testing."""
    with patch("agents.base.Config") as mc, \
         patch("agents.base.StructuredLogger") as ml, \
         patch("agents.base.AsyncHTTPClient") as mh, \
         patch("agents.base.RateLimiter") as mr, \
         patch("agents.base.DeadLetterQueue") as mdlq:

        default_config = {
            "associations": {
                "PMA": {"url": "https://pma.org", "priority": "high"},
                "NEMA": {"url": "https://nema.org", "priority": "medium"},
                "AGMA": {"url": "https://agma.org", "priority": "low"},
            }
        }
        config = associations_config or default_config

        mc.return_value.load.side_effect = lambda name: (
            config if name == "associations" else (agent_config or {})
        )

        from agents.orchestrator import OrchestratorAgent

        orch = OrchestratorAgent(
            agent_type="orchestrator",
            mode=mode,
            associations=associations or ["PMA"],
            dry_run=dry_run,
        )

        # Provide mock logger methods
        mock_log = MagicMock()
        orch.log = mock_log

        return orch


# =============================================================================
# TEST MAX_ERROR_RATE CONFIG
# =============================================================================


class TestMaxErrorRateConfig:
    """Tests for max_error_rate configuration."""

    def test_default_max_error_rate(self):
        """Default max_error_rate is 0.5 (50%)."""
        orch = _make_orchestrator()
        assert orch.max_error_rate == 0.5

    def test_custom_max_error_rate_from_config(self):
        """max_error_rate loaded from agent config."""
        orch = _make_orchestrator(
            agent_config={"orchestrator": {"max_extraction_errors": 0.1}}
        )
        assert orch.max_error_rate == 0.1

    def test_max_error_rate_as_string(self):
        """max_error_rate handles string values from YAML."""
        orch = _make_orchestrator(
            agent_config={"orchestrator": {"max_extraction_errors": "0.25"}}
        )
        assert orch.max_error_rate == 0.25


# =============================================================================
# TEST DISTRICT EXTRACTION ERROR RATE
# =============================================================================


class TestDistrictExtractionErrorRate:
    """Tests for error rate checking in _extract_district_directories()."""

    @pytest.mark.asyncio
    async def test_below_threshold_succeeds(self):
        """Error rate below threshold -> success."""
        orch = _make_orchestrator()
        orch.max_error_rate = 0.5

        # Mock spawner: 4 successes, 1 failure = 20% error rate
        results = [
            {"success": True, "records": [{"company_name": f"Co {i}"}]}
            for i in range(4)
        ] + [
            {"success": False, "error": "HTTP 502", "records": []}
        ]

        orch.spawner = MagicMock()
        orch.spawner.spawn_parallel = AsyncMock(return_value=results)

        config = {
            "district_urls": [f"https://pma.org/d{i}" for i in range(5)],
            "schema": "pma",
        }

        result = await orch._extract_district_directories("PMA", config)

        assert result["success"] is True
        assert result["records_extracted"] == 4

    @pytest.mark.asyncio
    async def test_above_threshold_fails(self):
        """Error rate above threshold -> failure with message."""
        orch = _make_orchestrator()
        orch.max_error_rate = 0.3  # 30% threshold

        # Mock spawner: 1 success, 4 failures = 80% error rate
        results = [
            {"success": True, "records": [{"company_name": "Co 1"}]}
        ] + [
            {"success": False, "error": "HTTP 502", "records": []}
            for _ in range(4)
        ]

        orch.spawner = MagicMock()
        orch.spawner.spawn_parallel = AsyncMock(return_value=results)

        config = {
            "district_urls": [f"https://pma.org/d{i}" for i in range(5)],
            "schema": "pma",
        }

        result = await orch._extract_district_directories("PMA", config)

        assert result["success"] is False
        assert "exceeds threshold" in result["error"]
        assert result["error_rate"] == 0.8
        assert result["failures"] == 4

    @pytest.mark.asyncio
    async def test_all_success_zero_error_rate(self):
        """All districts succeed -> 0% error rate."""
        orch = _make_orchestrator()
        orch.max_error_rate = 0.5

        results = [
            {"success": True, "records": [{"company_name": f"Co {i}", "member_id": f"M{i}"}]}
            for i in range(5)
        ]

        orch.spawner = MagicMock()
        orch.spawner.spawn_parallel = AsyncMock(return_value=results)

        config = {
            "district_urls": [f"https://pma.org/d{i}" for i in range(5)],
            "schema": "pma",
        }

        result = await orch._extract_district_directories("PMA", config)

        assert result["success"] is True
        assert result["records_extracted"] == 5

    @pytest.mark.asyncio
    async def test_all_failure_100_error_rate(self):
        """All districts fail -> 100% error rate."""
        orch = _make_orchestrator()
        orch.max_error_rate = 0.5

        results = [
            {"success": False, "error": "HTTP 502", "records": []}
            for _ in range(5)
        ]

        orch.spawner = MagicMock()
        orch.spawner.spawn_parallel = AsyncMock(return_value=results)

        config = {
            "district_urls": [f"https://pma.org/d{i}" for i in range(5)],
            "schema": "pma",
        }

        result = await orch._extract_district_directories("PMA", config)

        assert result["success"] is False
        assert result["error_rate"] == 1.0

    @pytest.mark.asyncio
    async def test_empty_district_urls(self):
        """Empty district_urls returns failure."""
        orch = _make_orchestrator()

        config = {"district_urls": [], "schema": "pma"}
        result = await orch._extract_district_directories("PMA", config)

        assert result["success"] is False
        assert "No district_urls" in result["error"]


# =============================================================================
# TEST AGGREGATE METRICS IN _run_extraction
# =============================================================================


class TestRunExtractionMetrics:
    """Tests for aggregate metrics in _run_extraction()."""

    @pytest.mark.asyncio
    async def test_aggregate_metrics_computed(self):
        """_run_extraction() computes total_errors, error_rate, associations_failed."""
        orch = _make_orchestrator(associations=["PMA", "NEMA"])
        orch.max_error_rate = 0.8  # High threshold so overall succeeds

        # Mock _extract_association
        call_count = 0

        async def mock_extract(association):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"success": True, "records_extracted": 100}
            return {"success": False, "error": "HTTP 502", "records_extracted": 0}

        orch._extract_association = mock_extract

        result = await orch._run_extraction(["PMA", "NEMA"])

        assert result["total_errors"] == 1
        assert result["associations_failed"] == 1
        assert result["error_rate"] == 0.5

    @pytest.mark.asyncio
    async def test_aggregate_error_rate_exceeds_threshold(self):
        """Overall failure when aggregate error rate exceeds threshold."""
        orch = _make_orchestrator(associations=["PMA", "NEMA", "AGMA"])
        orch.max_error_rate = 0.3  # Low threshold

        # All fail
        async def mock_extract(association):
            raise Exception(f"Failed to extract {association}")

        orch._extract_association = mock_extract

        result = await orch._run_extraction(["PMA", "NEMA", "AGMA"])

        assert result["success"] is False
        assert "exceeds threshold" in result.get("error", "")
        assert result["associations_failed"] == 3

    @pytest.mark.asyncio
    async def test_all_succeed_no_errors(self):
        """All associations succeed -> no errors in result."""
        orch = _make_orchestrator(associations=["PMA"])
        orch.max_error_rate = 0.5

        async def mock_extract(association):
            return {"success": True, "records_extracted": 50}

        orch._extract_association = mock_extract

        result = await orch._run_extraction(["PMA"])

        assert result["success"] is True
        assert result["total_errors"] == 0
        assert result["associations_failed"] == 0
        assert result["error_rate"] == 0.0

    @pytest.mark.asyncio
    async def test_exception_in_extract_counted_as_failure(self):
        """Exceptions in _extract_association are counted as failures."""
        orch = _make_orchestrator()
        orch.max_error_rate = 0.8  # High threshold

        async def mock_extract(association):
            raise RuntimeError("Connection refused")

        orch._extract_association = mock_extract

        result = await orch._run_extraction(["PMA"])

        assert result["associations_failed"] == 1


# =============================================================================
# TEST CLI --log-level FLAG
# =============================================================================


class TestCLILogLevel:
    """Tests for --log-level CLI flag."""

    def test_log_level_option_accepted(self):
        """CLI accepts --log-level option."""
        from agents.orchestrator import main

        runner = CliRunner()

        # This will fail because of missing config but should not fail
        # because of an unknown option
        result = runner.invoke(main, [
            "--mode", "extract",
            "--log-level", "DEBUG",
            "--dry-run",
        ])

        # Should not get "Error: No such option: --log-level"
        assert "No such option" not in (result.output or "")

    def test_log_level_debug(self):
        """--log-level DEBUG sets logger to DEBUG."""
        from agents.orchestrator import main

        runner = CliRunner()

        with patch("agents.orchestrator.OrchestratorAgent") as mock_cls:
            mock_instance = MagicMock()
            mock_instance.log = MagicMock()
            mock_instance.log.logger = MagicMock()
            mock_instance.execute = MagicMock(
                return_value={"success": True, "totals": {}}
            )
            mock_cls.return_value = mock_instance

            # asyncio.run needs a coroutine
            async def mock_execute(task):
                return {"success": True, "totals": {}}

            mock_instance.execute = mock_execute

            result = runner.invoke(main, [
                "--mode", "extract",
                "--log-level", "DEBUG",
                "--dry-run",
            ])

            # Check that setLevel was called with DEBUG
            mock_instance.log.logger.setLevel.assert_called_with(logging.DEBUG)

    def test_log_level_error(self):
        """--log-level ERROR sets logger to ERROR."""
        from agents.orchestrator import main

        runner = CliRunner()

        with patch("agents.orchestrator.OrchestratorAgent") as mock_cls:
            mock_instance = MagicMock()
            mock_instance.log = MagicMock()
            mock_instance.log.logger = MagicMock()

            async def mock_execute(task):
                return {"success": True, "totals": {}}

            mock_instance.execute = mock_execute
            mock_cls.return_value = mock_instance

            result = runner.invoke(main, [
                "--mode", "extract",
                "--log-level", "ERROR",
                "--dry-run",
            ])

            mock_instance.log.logger.setLevel.assert_called_with(logging.ERROR)

    def test_log_level_case_insensitive(self):
        """--log-level accepts lowercase values."""
        from agents.orchestrator import main

        runner = CliRunner()

        with patch("agents.orchestrator.OrchestratorAgent") as mock_cls:
            mock_instance = MagicMock()
            mock_instance.log = MagicMock()
            mock_instance.log.logger = MagicMock()

            async def mock_execute(task):
                return {"success": True, "totals": {}}

            mock_instance.execute = mock_execute
            mock_cls.return_value = mock_instance

            result = runner.invoke(main, [
                "--mode", "extract",
                "--log-level", "warning",
                "--dry-run",
            ])

            mock_instance.log.logger.setLevel.assert_called_with(logging.WARNING)

    def test_default_log_level_is_info(self):
        """Default --log-level is INFO."""
        from agents.orchestrator import main

        runner = CliRunner()

        with patch("agents.orchestrator.OrchestratorAgent") as mock_cls:
            mock_instance = MagicMock()
            mock_instance.log = MagicMock()
            mock_instance.log.logger = MagicMock()

            async def mock_execute(task):
                return {"success": True, "totals": {}}

            mock_instance.execute = mock_execute
            mock_cls.return_value = mock_instance

            result = runner.invoke(main, [
                "--mode", "extract",
                "--dry-run",
            ])

            mock_instance.log.logger.setLevel.assert_called_with(logging.INFO)


# =============================================================================
# TEST ERROR RATE EDGE CASES
# =============================================================================


class TestErrorRateEdgeCases:
    """Edge cases for error rate calculation."""

    @pytest.mark.asyncio
    async def test_single_district_failure(self):
        """Single district that fails = 100% error rate."""
        orch = _make_orchestrator()
        orch.max_error_rate = 0.5

        results = [{"success": False, "error": "HTTP 502", "records": []}]

        orch.spawner = MagicMock()
        orch.spawner.spawn_parallel = AsyncMock(return_value=results)

        config = {
            "district_urls": ["https://pma.org/d1"],
            "schema": "pma",
        }

        result = await orch._extract_district_directories("PMA", config)

        assert result["success"] is False
        assert result["error_rate"] == 1.0

    @pytest.mark.asyncio
    async def test_exactly_at_threshold(self):
        """Error rate exactly at threshold still passes (> not >=)."""
        orch = _make_orchestrator()
        orch.max_error_rate = 0.5  # 50% threshold

        # 1 fail out of 2 = exactly 50%
        results = [
            {"success": True, "records": [{"company_name": "Co 1", "member_id": "M1"}]},
            {"success": False, "error": "fail", "records": []},
        ]

        orch.spawner = MagicMock()
        orch.spawner.spawn_parallel = AsyncMock(return_value=results)

        config = {
            "district_urls": ["https://pma.org/d1", "https://pma.org/d2"],
            "schema": "pma",
        }

        result = await orch._extract_district_directories("PMA", config)

        # 50% == 0.5 threshold, so not > threshold -> success
        assert result["success"] is True
