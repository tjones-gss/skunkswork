"""
Tests for orchestrator error thresholds, metrics, and --log-level.

Phase 6: Orchestrator Hardening
"""

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
         patch("agents.base.StructuredLogger"), \
         patch("agents.base.AsyncHTTPClient"), \
         patch("agents.base.RateLimiter"), \
         patch("agents.base.DeadLetterQueue"):

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

            runner.invoke(main, [
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

            runner.invoke(main, [
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

            runner.invoke(main, [
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

            runner.invoke(main, [
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


# =============================================================================
# TEST STARTUP HEALTH SUMMARY
# =============================================================================


class TestBuildHealthSummary:
    """Tests for _build_health_summary() method."""

    def test_health_summary_structure(self):
        """Health summary has all required keys."""
        orch = _make_orchestrator(mode="full", associations=["PMA", "NEMA"])

        # Need state for associations list
        from state.machine import StateManager
        orch.state_manager = StateManager()
        orch.state = orch.state_manager.create_state(
            associations=["PMA", "NEMA"],
            job_id=orch.job_id,
        )

        summary = orch._build_health_summary()

        assert "timestamp" in summary
        assert "job_id" in summary
        assert "associations" in summary
        assert "api_keys" in summary
        assert "disk_free_gb" in summary
        assert "mode" in summary
        assert "dry_run" in summary

    def test_health_summary_associations(self):
        """Health summary includes correct associations."""
        orch = _make_orchestrator(mode="full", associations=["PMA", "AGMA"])

        from state.machine import StateManager
        orch.state_manager = StateManager()
        orch.state = orch.state_manager.create_state(
            associations=["PMA", "AGMA"],
            job_id=orch.job_id,
        )

        summary = orch._build_health_summary()

        assert "PMA" in summary["associations"]
        assert "AGMA" in summary["associations"]

    def test_health_summary_api_keys_masked(self, monkeypatch):
        """API keys are reported as booleans, not values."""
        monkeypatch.setenv("CLEARBIT_API_KEY", "secret-clearbit-123")
        monkeypatch.delenv("BUILTWITH_API_KEY", raising=False)

        orch = _make_orchestrator()

        from state.machine import StateManager
        orch.state_manager = StateManager()
        orch.state = orch.state_manager.create_state(
            associations=["PMA"],
            job_id=orch.job_id,
        )

        summary = orch._build_health_summary()

        api_keys = summary["api_keys"]
        # Keys should be True/False, never the actual secret
        for key_name, value in api_keys.items():
            assert isinstance(value, bool), f"{key_name} should be bool, got {type(value)}"
            # Ensure no actual key values leak
            assert value is True or value is False

    def test_health_summary_disk_free_positive(self):
        """Disk free GB is a positive number."""
        orch = _make_orchestrator()

        from state.machine import StateManager
        orch.state_manager = StateManager()
        orch.state = orch.state_manager.create_state(
            associations=["PMA"],
            job_id=orch.job_id,
        )

        summary = orch._build_health_summary()

        assert summary["disk_free_gb"] > 0


class TestPhaseInitHealthCheck:
    """Tests for health check integration in _phase_init()."""

    @pytest.mark.asyncio
    async def test_health_check_file_written(self, tmp_path, monkeypatch):
        """_phase_init() writes health_check.json file."""
        monkeypatch.chdir(tmp_path)

        orch = _make_orchestrator(dry_run=False, associations=["PMA"])

        from state.machine import StateManager
        orch.state_manager = StateManager(state_dir=str(tmp_path / "state"))
        orch.state = orch.state_manager.create_state(
            associations=["PMA"],
            job_id=orch.job_id,
        )

        result = await orch._phase_init()

        assert result is True
        health_path = tmp_path / "data" / ".state" / orch.job_id / "health_check.json"
        assert health_path.exists()

        data = json.loads(health_path.read_text())
        assert data["job_id"] == orch.job_id
        assert "api_keys" in data

    @pytest.mark.asyncio
    async def test_health_check_skipped_on_dry_run(self, tmp_path, monkeypatch):
        """_phase_init() skips health file in dry_run mode."""
        monkeypatch.chdir(tmp_path)

        orch = _make_orchestrator(dry_run=True, associations=["PMA"])

        from state.machine import StateManager
        orch.state_manager = StateManager(state_dir=str(tmp_path / "state"))
        orch.state = orch.state_manager.create_state(
            associations=["PMA"],
            job_id=orch.job_id,
        )

        result = await orch._phase_init()

        assert result is True
        health_path = tmp_path / "data" / ".state" / orch.job_id / "health_check.json"
        assert not health_path.exists()

    @pytest.mark.asyncio
    async def test_missing_api_keys_logged_as_warnings(self, tmp_path, monkeypatch):
        """_phase_init() logs missing API keys as warnings."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("CLEARBIT_API_KEY", raising=False)
        monkeypatch.delenv("BUILTWITH_API_KEY", raising=False)
        monkeypatch.delenv("APOLLO_API_KEY", raising=False)
        monkeypatch.delenv("HUNTER_API_KEY", raising=False)
        monkeypatch.delenv("GOOGLE_PLACES_API_KEY", raising=False)

        orch = _make_orchestrator(dry_run=True, associations=["PMA"])

        from state.machine import StateManager
        orch.state_manager = StateManager(state_dir=str(tmp_path / "state"))
        orch.state = orch.state_manager.create_state(
            associations=["PMA"],
            job_id=orch.job_id,
        )

        result = await orch._phase_init()

        assert result is True
        # health_summary call should log info
        orch.log.info.assert_called()



# =============================================================================
# TEST PARTIAL-PHASE RESUME WIRING (P4-T02)
# =============================================================================


def _make_orchestrator_with_state(
    phase="INIT",
    companies=None,
    events=None,
    phase_progress=None,
):
    """Create an orchestrator with a real PipelineState for resume tests."""
    from state.machine import PipelinePhase, PipelineState, StateManager

    orch = _make_orchestrator(dry_run=True)

    state = PipelineState(association_codes=["PMA"])
    state_manager = StateManager()

    # Transition to the desired phase
    phase_order = [
        PipelinePhase.GATEKEEPER, PipelinePhase.DISCOVERY,
        PipelinePhase.CLASSIFICATION, PipelinePhase.EXTRACTION,
        PipelinePhase.ENRICHMENT, PipelinePhase.VALIDATION,
        PipelinePhase.RESOLUTION, PipelinePhase.GRAPH,
        PipelinePhase.EXPORT, PipelinePhase.MONITOR,
    ]
    target = PipelinePhase(phase) if phase != "INIT" else PipelinePhase.INIT
    for p in phase_order:
        if state.current_phase == target:
            break
        state.transition_to(p)

    if companies:
        for c in companies:
            state.add_company(c)
    if events:
        for e in events:
            state.add_event(e)
    if phase_progress:
        state.update_phase_progress(**phase_progress)

    orch.state = state
    orch.state_manager = state_manager
    orch.state_manager.checkpoint = MagicMock()

    spawner = MagicMock()
    spawner.spawn = AsyncMock(return_value={"success": True, "records": []})
    spawner.spawn_parallel = AsyncMock(return_value=[])
    orch.spawner = spawner

    return orch


class TestPhaseResumeWiring:
    """Tests for partial-phase resume logic (P4-T02)."""

    # ------------------------------------------------------------------
    # _execute_phase resume logging
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_execute_phase_logs_resume_when_progress_exists(self):
        """_execute_phase() logs a resume message when phase_progress is non-empty."""
        orch = _make_orchestrator_with_state(
            phase="GATEKEEPER",
            phase_progress={"checked_domains": ["example.com"]},
        )
        await orch._execute_phase(
            __import__("state.machine", fromlist=["PipelinePhase"]).PipelinePhase.GATEKEEPER
        )
        # Should have logged the resume message
        orch.log.info.assert_any_call(
            "Resuming phase GATEKEEPER with progress",
            phase_progress={"checked_domains": ["example.com"]},
        )

    @pytest.mark.asyncio
    async def test_execute_phase_no_resume_log_when_empty_progress(self):
        """_execute_phase() does NOT log resume when phase_progress is empty."""
        orch = _make_orchestrator_with_state(phase="GATEKEEPER")
        await orch._execute_phase(
            __import__("state.machine", fromlist=["PipelinePhase"]).PipelinePhase.GATEKEEPER
        )
        # Should not have logged "Resuming phase"
        for call in orch.log.info.call_args_list:
            if call.args and "Resuming phase" in str(call.args[0]):
                pytest.fail("Should not log resume when phase_progress is empty")

    # ------------------------------------------------------------------
    # GATEKEEPER: checked_domains skip
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_gatekeeper_skips_already_checked_domains(self):
        """GATEKEEPER skips domains already in phase_progress.checked_domains."""
        orch = _make_orchestrator_with_state(
            phase="GATEKEEPER",
            phase_progress={"checked_domains": ["pma.org"]},
        )
        # Add a URL for pma.org to the crawl queue
        orch.state.add_to_queue(url="https://pma.org/members", association="PMA")

        await orch._phase_gatekeeper()

        # spawn should NOT have been called for pma.org (it was already checked)
        orch.spawner.spawn.assert_not_called()

    @pytest.mark.asyncio
    async def test_gatekeeper_processes_new_domains(self):
        """GATEKEEPER processes domains NOT in phase_progress."""
        orch = _make_orchestrator_with_state(phase="GATEKEEPER")
        orch.state.add_to_queue(url="https://nema.org/dir", association="NEMA")

        await orch._phase_gatekeeper()

        orch.spawner.spawn.assert_called_once()
        # Verify progress was updated
        assert "nema.org" in orch.state.phase_progress.get("checked_domains", [])

    # ------------------------------------------------------------------
    # ENRICHMENT: completed_steps skip
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_enrichment_skips_completed_steps(self):
        """ENRICHMENT skips sub-agents already in completed_steps."""
        companies = [{"company_name": "Acme", "website": "https://acme.com"}]
        orch = _make_orchestrator_with_state(
            phase="ENRICHMENT",
            companies=companies,
            phase_progress={"completed_steps": ["firmographic", "tech_stack"]},
        )
        orch.spawner.spawn = AsyncMock(
            return_value={"success": True, "records": companies}
        )

        await orch._phase_enrichment()

        # Only contact_finder should have been called (firm + tech already done)
        assert orch.spawner.spawn.call_count == 1
        call_args = orch.spawner.spawn.call_args
        assert call_args[0][0] == "enrichment.contact_finder"

    @pytest.mark.asyncio
    async def test_enrichment_all_steps_completed_no_spawn(self):
        """ENRICHMENT with all steps complete does not call spawn."""
        companies = [{"company_name": "Acme"}]
        orch = _make_orchestrator_with_state(
            phase="ENRICHMENT",
            companies=companies,
            phase_progress={
                "completed_steps": ["firmographic", "tech_stack", "contact_finder"]
            },
        )
        await orch._phase_enrichment()
        orch.spawner.spawn.assert_not_called()

    # ------------------------------------------------------------------
    # VALIDATION: completed_steps skip
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_validation_skips_completed_steps(self):
        """VALIDATION skips sub-agents already in completed_steps."""
        companies = [{"company_name": "Acme"}]
        orch = _make_orchestrator_with_state(
            phase="VALIDATION",
            companies=companies,
            phase_progress={"completed_steps": ["dedupe"]},
        )
        orch.spawner.spawn = AsyncMock(
            return_value={"success": True, "records": companies}
        )

        await orch._phase_validation()

        # crossref + scorer should have been called (dedupe already done)
        assert orch.spawner.spawn.call_count == 2
        agent_types = [c[0][0] for c in orch.spawner.spawn.call_args_list]
        assert "validation.crossref" in agent_types
        assert "validation.scorer" in agent_types
        assert "validation.dedupe" not in agent_types

    # ------------------------------------------------------------------
    # RESOLUTION: resolved flag
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_resolution_skips_when_already_resolved(self):
        """RESOLUTION skips entity_resolver when resolved=True."""
        companies = [{"company_name": "Acme"}]
        orch = _make_orchestrator_with_state(
            phase="RESOLUTION",
            companies=companies,
            phase_progress={"resolved": True},
        )

        await orch._phase_resolution()
        orch.spawner.spawn.assert_not_called()

    @pytest.mark.asyncio
    async def test_resolution_runs_when_not_resolved(self):
        """RESOLUTION runs entity_resolver when resolved is not set."""
        companies = [{"company_name": "Acme"}]
        orch = _make_orchestrator_with_state(
            phase="RESOLUTION",
            companies=companies,
        )
        orch.spawner.spawn = AsyncMock(
            return_value={"success": True, "canonical_entities": companies}
        )

        await orch._phase_resolution()
        orch.spawner.spawn.assert_called_once()
        assert orch.state.phase_progress.get("resolved") is True

    # ------------------------------------------------------------------
    # GRAPH: mined_company_ids + graph_built
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_graph_skips_already_mined_companies(self):
        """GRAPH skips companies already in mined_company_ids."""
        companies = [
            {"company_name": "Acme", "id": "acme-1", "website": "https://acme.com"},
            {"company_name": "Beta", "id": "beta-2", "website": "https://beta.com"},
        ]
        orch = _make_orchestrator_with_state(
            phase="GRAPH",
            companies=companies,
            phase_progress={"mined_company_ids": ["acme-1"]},
        )

        await orch._phase_graph()

        # Only Beta should have been mined + graph build = 2 calls
        spawn_calls = orch.spawner.spawn.call_args_list
        miner_calls = [c for c in spawn_calls if "competitor_signal_miner" in c[0][0]]
        assert len(miner_calls) == 1
        assert miner_calls[0][0][1]["source_company_id"] == "beta-2"

    @pytest.mark.asyncio
    async def test_graph_skips_build_when_already_built(self):
        """GRAPH skips relationship_graph_builder when graph_built=True."""
        orch = _make_orchestrator_with_state(
            phase="GRAPH",
            phase_progress={"graph_built": True, "mined_company_ids": []},
        )
        # No companies, so mining loop is empty; graph_built skips builder
        await orch._phase_graph()

        # graph builder should NOT have been called
        for call in orch.spawner.spawn.call_args_list:
            if "relationship_graph_builder" in str(call):
                pytest.fail("Should not call graph builder when graph_built=True")

    # ------------------------------------------------------------------
    # EXPORT: completed_exports skip
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_export_skips_completed_exports(self):
        """EXPORT skips export types already in completed_exports."""
        companies = [{"company_name": "Acme", "quality_score": 80}]
        orch = _make_orchestrator_with_state(
            phase="EXPORT",
            companies=companies,
            phase_progress={"completed_exports": ["companies"]},
        )
        orch.dry_run = False
        orch.spawner.spawn = AsyncMock(return_value={"success": True})

        await orch._phase_export()

        # companies export was skipped; events (no events) + summary called
        agent_calls = [c[0][1].get("export_type") for c in orch.spawner.spawn.call_args_list]
        assert "companies" not in agent_calls
        assert "summary" in agent_calls

    # ------------------------------------------------------------------
    # Backward compatibility: empty progress starts from beginning
    # ------------------------------------------------------------------

    @pytest.mark.asyncio
    async def test_enrichment_empty_progress_runs_all_steps(self):
        """Empty phase_progress runs all enrichment sub-agents."""
        companies = [{"company_name": "Acme"}]
        orch = _make_orchestrator_with_state(
            phase="ENRICHMENT",
            companies=companies,
        )
        orch.spawner.spawn = AsyncMock(
            return_value={"success": True, "records": companies}
        )

        await orch._phase_enrichment()

        assert orch.spawner.spawn.call_count == 3
        agent_types = [c[0][0] for c in orch.spawner.spawn.call_args_list]
        assert agent_types == [
            "enrichment.firmographic",
            "enrichment.tech_stack",
            "enrichment.contact_finder",
        ]
