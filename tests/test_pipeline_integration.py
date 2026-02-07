"""
Pipeline Integration Tests
NAM Intelligence Pipeline

Integration tests for pipeline phase transitions, state management, and orchestration.
"""

import json
from datetime import datetime, UTC
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================================
# TEST: Pipeline Phase Transitions
# =============================================================================


class TestPipelinePhaseTransitions:
    """Tests for pipeline phase transition logic."""

    def test_valid_init_to_gatekeeper(self, fresh_pipeline_state):
        """INIT can transition to GATEKEEPER."""
        from state.machine import PipelinePhase

        result = fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)

        assert result is True
        assert fresh_pipeline_state.current_phase == PipelinePhase.GATEKEEPER

    def test_valid_init_to_failed(self, fresh_pipeline_state):
        """INIT can transition to FAILED."""
        from state.machine import PipelinePhase

        result = fresh_pipeline_state.transition_to(PipelinePhase.FAILED)

        assert result is True
        assert fresh_pipeline_state.current_phase == PipelinePhase.FAILED

    def test_invalid_init_to_extraction(self, fresh_pipeline_state):
        """INIT cannot skip to EXTRACTION."""
        from state.machine import PipelinePhase

        result = fresh_pipeline_state.transition_to(PipelinePhase.EXTRACTION)

        assert result is False
        assert fresh_pipeline_state.current_phase == PipelinePhase.INIT

    def test_invalid_init_to_done(self, fresh_pipeline_state):
        """INIT cannot skip to DONE."""
        from state.machine import PipelinePhase

        result = fresh_pipeline_state.transition_to(PipelinePhase.DONE)

        assert result is False
        assert fresh_pipeline_state.current_phase == PipelinePhase.INIT

    def test_done_is_terminal(self, fresh_pipeline_state):
        """DONE cannot transition to any other phase."""
        from state.machine import PipelinePhase, PHASE_TRANSITIONS

        # Force to DONE state
        fresh_pipeline_state.current_phase = PipelinePhase.DONE

        # Try all phases
        for phase in PipelinePhase:
            result = fresh_pipeline_state.transition_to(phase)
            assert result is False

        assert fresh_pipeline_state.current_phase == PipelinePhase.DONE

    def test_failed_is_terminal(self, fresh_pipeline_state):
        """FAILED cannot transition to any other phase."""
        from state.machine import PipelinePhase

        fresh_pipeline_state.current_phase = PipelinePhase.FAILED

        for phase in PipelinePhase:
            result = fresh_pipeline_state.transition_to(phase)
            assert result is False

        assert fresh_pipeline_state.current_phase == PipelinePhase.FAILED

    def test_full_happy_path_transitions(self, fresh_pipeline_state):
        """Full pipeline happy path transitions succeed."""
        from state.machine import PipelinePhase

        transitions = [
            PipelinePhase.GATEKEEPER,
            PipelinePhase.DISCOVERY,
            PipelinePhase.CLASSIFICATION,
            PipelinePhase.EXTRACTION,
            PipelinePhase.ENRICHMENT,
            PipelinePhase.VALIDATION,
            PipelinePhase.RESOLUTION,
            PipelinePhase.GRAPH,
            PipelinePhase.EXPORT,
            PipelinePhase.MONITOR,
            PipelinePhase.DONE,
        ]

        for expected_phase in transitions:
            result = fresh_pipeline_state.transition_to(expected_phase)
            assert result is True, f"Failed to transition to {expected_phase}"
            assert fresh_pipeline_state.current_phase == expected_phase

    def test_any_phase_can_fail(self, fresh_pipeline_state):
        """Any phase (except terminal) can transition to FAILED."""
        from state.machine import PipelinePhase, PHASE_TRANSITIONS

        for phase, valid_transitions in PHASE_TRANSITIONS.items():
            if phase not in [PipelinePhase.DONE, PipelinePhase.FAILED]:
                assert PipelinePhase.FAILED in valid_transitions, \
                    f"{phase} should be able to transition to FAILED"

    def test_transition_sets_phase_started_at(self, fresh_pipeline_state):
        """Transition sets phase_started_at timestamp."""
        from state.machine import PipelinePhase

        before = datetime.now(UTC)
        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)
        after = datetime.now(UTC)

        assert fresh_pipeline_state.phase_started_at is not None
        assert before <= fresh_pipeline_state.phase_started_at <= after

    def test_transition_updates_updated_at(self, fresh_pipeline_state):
        """Transition updates updated_at timestamp."""
        from state.machine import PipelinePhase

        original = fresh_pipeline_state.updated_at
        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)

        assert fresh_pipeline_state.updated_at >= original

    def test_done_sets_completed_at(self, fresh_pipeline_state):
        """Transitioning to DONE sets completed_at."""
        from state.machine import PipelinePhase

        # Navigate to DONE
        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)
        fresh_pipeline_state.transition_to(PipelinePhase.DISCOVERY)
        fresh_pipeline_state.transition_to(PipelinePhase.CLASSIFICATION)
        fresh_pipeline_state.transition_to(PipelinePhase.EXTRACTION)
        fresh_pipeline_state.transition_to(PipelinePhase.ENRICHMENT)
        fresh_pipeline_state.transition_to(PipelinePhase.VALIDATION)
        fresh_pipeline_state.transition_to(PipelinePhase.RESOLUTION)
        fresh_pipeline_state.transition_to(PipelinePhase.GRAPH)
        fresh_pipeline_state.transition_to(PipelinePhase.EXPORT)
        fresh_pipeline_state.transition_to(PipelinePhase.DONE)

        assert fresh_pipeline_state.completed_at is not None

    def test_export_can_go_to_done_or_monitor(self, fresh_pipeline_state):
        """EXPORT can transition to either DONE or MONITOR."""
        from state.machine import PipelinePhase, PHASE_TRANSITIONS

        valid = PHASE_TRANSITIONS[PipelinePhase.EXPORT]

        assert PipelinePhase.DONE in valid
        assert PipelinePhase.MONITOR in valid


# =============================================================================
# TEST: Pipeline State Data Buckets
# =============================================================================


class TestPipelineStateDataBuckets:
    """Tests for pipeline state data bucket operations."""

    def test_add_to_queue(self, fresh_pipeline_state):
        """add_to_queue adds URL to crawl queue."""
        fresh_pipeline_state.add_to_queue("https://test.com")

        assert len(fresh_pipeline_state.crawl_queue) == 1
        assert fresh_pipeline_state.crawl_queue[0]["url"] == "https://test.com"

    def test_add_to_queue_with_priority(self, fresh_pipeline_state):
        """add_to_queue accepts priority parameter."""
        fresh_pipeline_state.add_to_queue("https://test.com", priority=10)

        assert fresh_pipeline_state.crawl_queue[0]["priority"] == 10

    def test_add_to_queue_with_kwargs(self, fresh_pipeline_state):
        """add_to_queue accepts additional kwargs."""
        fresh_pipeline_state.add_to_queue(
            "https://test.com",
            priority=5,
            association="PMA",
            depth=1
        )

        item = fresh_pipeline_state.crawl_queue[0]
        assert item["association"] == "PMA"
        assert item["depth"] == 1

    def test_add_to_queue_increments_counter(self, fresh_pipeline_state):
        """add_to_queue increments total_urls_discovered."""
        assert fresh_pipeline_state.total_urls_discovered == 0

        fresh_pipeline_state.add_to_queue("https://test1.com")
        fresh_pipeline_state.add_to_queue("https://test2.com")

        assert fresh_pipeline_state.total_urls_discovered == 2

    def test_add_to_queue_skips_visited(self, fresh_pipeline_state):
        """add_to_queue skips URLs already visited."""
        fresh_pipeline_state.mark_visited("https://test.com")
        fresh_pipeline_state.add_to_queue("https://test.com")

        assert len(fresh_pipeline_state.crawl_queue) == 0

    def test_add_to_queue_skips_blocked(self, fresh_pipeline_state):
        """add_to_queue skips URLs already blocked."""
        fresh_pipeline_state.mark_blocked("https://test.com")
        fresh_pipeline_state.add_to_queue("https://test.com")

        assert len(fresh_pipeline_state.crawl_queue) == 0

    def test_add_to_queue_skips_duplicates(self, fresh_pipeline_state):
        """add_to_queue skips URLs already in queue."""
        fresh_pipeline_state.add_to_queue("https://test.com")
        fresh_pipeline_state.add_to_queue("https://test.com")

        assert len(fresh_pipeline_state.crawl_queue) == 1

    def test_get_next_url_returns_highest_priority(self, fresh_pipeline_state):
        """get_next_url returns URL with highest priority."""
        fresh_pipeline_state.add_to_queue("https://low.com", priority=1)
        fresh_pipeline_state.add_to_queue("https://high.com", priority=10)
        fresh_pipeline_state.add_to_queue("https://medium.com", priority=5)

        next_item = fresh_pipeline_state.get_next_url()

        assert next_item["url"] == "https://high.com"

    def test_get_next_url_removes_from_queue(self, fresh_pipeline_state):
        """get_next_url removes item from queue."""
        fresh_pipeline_state.add_to_queue("https://test.com")

        assert len(fresh_pipeline_state.crawl_queue) == 1
        fresh_pipeline_state.get_next_url()
        assert len(fresh_pipeline_state.crawl_queue) == 0

    def test_get_next_url_empty_queue_returns_none(self, fresh_pipeline_state):
        """get_next_url returns None for empty queue."""
        result = fresh_pipeline_state.get_next_url()

        assert result is None

    def test_mark_visited_adds_to_list(self, fresh_pipeline_state):
        """mark_visited adds URL to visited_urls."""
        fresh_pipeline_state.mark_visited("https://test.com")

        assert "https://test.com" in fresh_pipeline_state.visited_urls

    def test_mark_visited_increments_counter(self, fresh_pipeline_state):
        """mark_visited increments total_pages_fetched."""
        assert fresh_pipeline_state.total_pages_fetched == 0

        fresh_pipeline_state.mark_visited("https://test.com")

        assert fresh_pipeline_state.total_pages_fetched == 1

    def test_mark_visited_skips_duplicates(self, fresh_pipeline_state):
        """mark_visited skips already-visited URLs."""
        fresh_pipeline_state.mark_visited("https://test.com")
        fresh_pipeline_state.mark_visited("https://test.com")

        assert len(fresh_pipeline_state.visited_urls) == 1
        assert fresh_pipeline_state.total_pages_fetched == 1

    def test_mark_blocked_adds_to_list(self, fresh_pipeline_state):
        """mark_blocked adds URL to blocked_urls."""
        fresh_pipeline_state.mark_blocked("https://test.com", reason="robots.txt")

        assert "https://test.com" in fresh_pipeline_state.blocked_urls

    def test_add_company_increments_counter(self, fresh_pipeline_state):
        """add_company increments total_companies_extracted."""
        assert fresh_pipeline_state.total_companies_extracted == 0

        fresh_pipeline_state.add_company({"company_name": "Test Corp"})

        assert fresh_pipeline_state.total_companies_extracted == 1

    def test_add_event_increments_counter(self, fresh_pipeline_state):
        """add_event increments total_events_extracted."""
        assert fresh_pipeline_state.total_events_extracted == 0

        fresh_pipeline_state.add_event({"title": "Test Event"})

        assert fresh_pipeline_state.total_events_extracted == 1

    def test_add_participant_increments_counter(self, fresh_pipeline_state):
        """add_participant increments total_participants_extracted."""
        assert fresh_pipeline_state.total_participants_extracted == 0

        fresh_pipeline_state.add_participant({"company_name": "Exhibitor"})

        assert fresh_pipeline_state.total_participants_extracted == 1

    def test_add_signal_increments_counter(self, fresh_pipeline_state):
        """add_signal increments total_signals_detected."""
        assert fresh_pipeline_state.total_signals_detected == 0

        fresh_pipeline_state.add_signal({"competitor": "SAP", "type": "mention"})

        assert fresh_pipeline_state.total_signals_detected == 1

    def test_add_canonical_entity_increments_counter(self, fresh_pipeline_state):
        """add_canonical_entity increments total_entities_resolved."""
        assert fresh_pipeline_state.total_entities_resolved == 0

        fresh_pipeline_state.add_canonical_entity({"id": "entity-1"})

        assert fresh_pipeline_state.total_entities_resolved == 1


# =============================================================================
# TEST: Pipeline State Summary
# =============================================================================


class TestPipelineStateSummary:
    """Tests for pipeline state summary generation."""

    def test_get_summary_contains_required_fields(self, fresh_pipeline_state):
        """get_summary returns all required fields."""
        summary = fresh_pipeline_state.get_summary()

        assert "job_id" in summary
        assert "associations" in summary
        assert "current_phase" in summary
        assert "queue_size" in summary
        assert "visited_urls" in summary
        assert "blocked_urls" in summary
        assert "pages_fetched" in summary
        assert "companies_extracted" in summary
        assert "events_extracted" in summary
        assert "participants_extracted" in summary
        assert "signals_detected" in summary
        assert "entities_resolved" in summary
        assert "errors" in summary
        assert "created_at" in summary
        assert "updated_at" in summary
        assert "completed_at" in summary

    def test_get_summary_reflects_data(self, populated_pipeline_state):
        """get_summary reflects current state data."""
        summary = populated_pipeline_state.get_summary()

        assert summary["job_id"] == "test-job-123"
        assert summary["associations"] == ["PMA"]
        assert summary["queue_size"] == 2
        assert summary["visited_urls"] == 1
        assert summary["companies_extracted"] == 1
        assert summary["events_extracted"] == 1

    def test_get_summary_completed_at_formatting(self, fresh_pipeline_state):
        """get_summary formats completed_at correctly."""
        summary = fresh_pipeline_state.get_summary()
        assert summary["completed_at"] is None

        fresh_pipeline_state.completed_at = datetime.now(UTC)
        summary = fresh_pipeline_state.get_summary()
        assert summary["completed_at"] is not None
        assert isinstance(summary["completed_at"], str)


# =============================================================================
# TEST: State Manager Persistence
# =============================================================================


class TestStateManagerPersistence:
    """Tests for StateManager persistence functionality."""

    def test_save_and_load_preserves_state(self, state_manager, populated_pipeline_state):
        """State is preserved across save/load cycle."""
        state_manager.save_state(populated_pipeline_state)
        loaded = state_manager.load_state(populated_pipeline_state.job_id)

        assert loaded.job_id == populated_pipeline_state.job_id
        assert loaded.association_codes == populated_pipeline_state.association_codes
        assert len(loaded.crawl_queue) == len(populated_pipeline_state.crawl_queue)
        assert len(loaded.visited_urls) == len(populated_pipeline_state.visited_urls)
        assert loaded.total_companies_extracted == populated_pipeline_state.total_companies_extracted

    def test_checkpoint_preserves_phase(self, state_manager, fresh_pipeline_state):
        """Checkpoint preserves current phase."""
        from state.machine import PipelinePhase

        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)
        fresh_pipeline_state.transition_to(PipelinePhase.DISCOVERY)
        state_manager.checkpoint(fresh_pipeline_state)

        loaded = state_manager.load_state(fresh_pipeline_state.job_id)

        assert loaded.current_phase == PipelinePhase.DISCOVERY

    def test_multiple_checkpoints(self, state_manager, fresh_pipeline_state):
        """Multiple checkpoints can be created."""
        from state.machine import PipelinePhase
        import time

        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)
        state_manager.checkpoint(fresh_pipeline_state)

        time.sleep(0.01)

        fresh_pipeline_state.transition_to(PipelinePhase.DISCOVERY)
        state_manager.checkpoint(fresh_pipeline_state)

        checkpoints = list(state_manager.state_dir.glob("*.checkpoint.json"))
        assert len(checkpoints) == 2

    def test_list_jobs_sorted_by_updated_at(self, state_manager):
        """list_jobs returns jobs sorted by updated_at descending."""
        import time

        state1 = state_manager.create_state(["PMA"], job_id="older-job")
        time.sleep(0.01)
        state2 = state_manager.create_state(["NEMA"], job_id="newer-job")

        jobs = state_manager.list_jobs()

        assert len(jobs) == 2
        assert jobs[0]["job_id"] == "newer-job"
        assert jobs[1]["job_id"] == "older-job"


# =============================================================================
# TEST: Phase History
# =============================================================================


class TestPhaseHistory:
    """Tests for phase history recording."""

    def test_transition_records_history(self, fresh_pipeline_state):
        """Transitions record phase history."""
        from state.machine import PipelinePhase

        # Set up started_at for current phase
        fresh_pipeline_state.phase_started_at = datetime.now(UTC)

        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)

        assert len(fresh_pipeline_state.phase_history) == 1
        history = fresh_pipeline_state.phase_history[0]
        assert history["phase"] == PipelinePhase.INIT

    def test_history_includes_timestamps(self, fresh_pipeline_state):
        """Phase history includes start and end timestamps."""
        from state.machine import PipelinePhase

        fresh_pipeline_state.phase_started_at = datetime.now(UTC)
        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)

        history = fresh_pipeline_state.phase_history[0]

        assert "started_at" in history
        assert "ended_at" in history

    def test_history_includes_stats(self, fresh_pipeline_state):
        """Phase history includes stats snapshot."""
        from state.machine import PipelinePhase

        fresh_pipeline_state.phase_started_at = datetime.now(UTC)
        fresh_pipeline_state.add_to_queue("https://test.com")
        fresh_pipeline_state.add_company({"company_name": "Test"})

        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)

        history = fresh_pipeline_state.phase_history[0]

        assert "stats" in history
        assert history["stats"]["urls_discovered"] == 1
        assert history["stats"]["companies"] == 1

    def test_multiple_transitions_build_history(self, fresh_pipeline_state):
        """Multiple transitions build complete history."""
        from state.machine import PipelinePhase

        fresh_pipeline_state.phase_started_at = datetime.now(UTC)

        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)
        fresh_pipeline_state.transition_to(PipelinePhase.DISCOVERY)
        fresh_pipeline_state.transition_to(PipelinePhase.CLASSIFICATION)

        assert len(fresh_pipeline_state.phase_history) == 3
        phases = [h["phase"] for h in fresh_pipeline_state.phase_history]
        assert PipelinePhase.INIT in phases
        assert PipelinePhase.GATEKEEPER in phases
        assert PipelinePhase.DISCOVERY in phases


# =============================================================================
# TEST: Orchestrator Agent Mocking
# =============================================================================


class TestOrchestratorAgentMocked:
    """Tests for orchestrator with mocked agents."""

    @pytest.mark.asyncio
    async def test_spawn_returns_success_result(self, mock_agent_spawner):
        """Mocked spawner returns success result."""
        result = await mock_agent_spawner.spawn(
            "discovery.access_gatekeeper",
            {"urls": ["https://test.com"]}
        )

        assert result["success"] is True
        assert result["records_processed"] == 10
        assert "_meta" in result
        assert result["_meta"]["agent_type"] == "discovery.access_gatekeeper"

    @pytest.mark.asyncio
    async def test_spawn_parallel_returns_results(self, mock_agent_spawner):
        """Mocked spawner returns results for parallel execution."""
        tasks = [
            {"url": "https://test1.com"},
            {"url": "https://test2.com"},
            {"url": "https://test3.com"}
        ]

        results = await mock_agent_spawner.spawn_parallel(
            "extraction.html_parser",
            tasks
        )

        assert len(results) == 3
        assert all(r["success"] for r in results)

    @pytest.mark.asyncio
    async def test_failing_spawner_returns_error(self, mock_failing_spawner):
        """Failing spawner returns error result."""
        result = await mock_failing_spawner.spawn(
            "discovery.access_gatekeeper",
            {"urls": ["https://test.com"]}
        )

        assert result["success"] is False
        assert "error" in result
        assert result["error_type"] == "TestError"


# =============================================================================
# TEST: Agent Spawner Mocking
# =============================================================================


class TestAgentSpawnerMocking:
    """Tests for AgentSpawner mocking patterns."""

    @pytest.mark.asyncio
    async def test_mock_spawn_call_tracking(self, mock_agent_spawner):
        """Mock spawner tracks call arguments."""
        await mock_agent_spawner.spawn(
            "discovery.site_mapper",
            {"seed_url": "https://pma.org"}
        )

        mock_agent_spawner.spawn.assert_called_once()
        call_args = mock_agent_spawner.spawn.call_args
        assert call_args[0][0] == "discovery.site_mapper"
        assert call_args[0][1]["seed_url"] == "https://pma.org"

    @pytest.mark.asyncio
    async def test_mock_spawn_parallel_call_tracking(self, mock_agent_spawner):
        """Mock spawner tracks parallel call arguments."""
        tasks = [{"url": "https://a.com"}, {"url": "https://b.com"}]

        await mock_agent_spawner.spawn_parallel(
            "extraction.html_parser",
            tasks,
            max_concurrent=3
        )

        mock_agent_spawner.spawn_parallel.assert_called_once()

    def test_mock_spawner_has_job_id(self, mock_agent_spawner):
        """Mock spawner has job_id attribute."""
        assert mock_agent_spawner.job_id is not None
        assert len(mock_agent_spawner.job_id) == 36


# =============================================================================
# TEST: End-to-End Pipeline Flow
# =============================================================================


class TestEndToEndPipelineFlow:
    """Tests for end-to-end pipeline execution flow."""

    @pytest.mark.asyncio
    async def test_full_pipeline_flow_with_mocked_agents(
        self,
        state_manager,
        mock_agent_spawner
    ):
        """Full pipeline flow with mocked agents."""
        from state.machine import PipelinePhase

        # Create state
        state = state_manager.create_state(["PMA"], job_id="e2e-test")

        # Simulate GATEKEEPER phase
        state_manager.transition_phase(state, PipelinePhase.GATEKEEPER)
        result = await mock_agent_spawner.spawn(
            "discovery.access_gatekeeper",
            {"urls": ["https://pma.org"]}
        )
        assert result["success"]
        state.add_to_queue("https://pma.org/members")

        # Simulate DISCOVERY phase
        state_manager.transition_phase(state, PipelinePhase.DISCOVERY)
        result = await mock_agent_spawner.spawn(
            "discovery.site_mapper",
            {"seed_url": "https://pma.org"}
        )
        assert result["success"]

        # Simulate CLASSIFICATION phase
        state_manager.transition_phase(state, PipelinePhase.CLASSIFICATION)
        result = await mock_agent_spawner.spawn(
            "discovery.page_classifier",
            {"pages": []}
        )
        assert result["success"]

        # Simulate EXTRACTION phase
        state_manager.transition_phase(state, PipelinePhase.EXTRACTION)
        result = await mock_agent_spawner.spawn(
            "extraction.html_parser",
            {"pages": [], "association_code": "PMA"}
        )
        state.add_company({"company_name": "Test Company"})

        # Simulate ENRICHMENT phase
        state_manager.transition_phase(state, PipelinePhase.ENRICHMENT)

        # Simulate VALIDATION phase
        state_manager.transition_phase(state, PipelinePhase.VALIDATION)

        # Simulate RESOLUTION phase
        state_manager.transition_phase(state, PipelinePhase.RESOLUTION)

        # Simulate GRAPH phase
        state_manager.transition_phase(state, PipelinePhase.GRAPH)

        # Simulate EXPORT phase
        state_manager.transition_phase(state, PipelinePhase.EXPORT)

        # Complete
        state_manager.transition_phase(state, PipelinePhase.DONE)

        assert state.current_phase == PipelinePhase.DONE
        assert state.completed_at is not None
        assert state.total_companies_extracted == 1

    @pytest.mark.asyncio
    async def test_pipeline_failure_handling(
        self,
        state_manager,
        mock_failing_spawner
    ):
        """Pipeline handles agent failures correctly."""
        from state.machine import PipelinePhase

        state = state_manager.create_state(["PMA"], job_id="failure-test")
        state_manager.transition_phase(state, PipelinePhase.GATEKEEPER)

        # Agent fails
        result = await mock_failing_spawner.spawn(
            "discovery.access_gatekeeper",
            {"urls": ["https://pma.org"]}
        )

        assert result["success"] is False

        # Record error
        state.add_error({
            "phase": "GATEKEEPER",
            "agent": "access_gatekeeper",
            "error_type": result["error_type"],
            "error_message": result["error"]
        })

        # Transition to FAILED
        state_manager.transition_phase(state, PipelinePhase.FAILED)

        assert state.current_phase == PipelinePhase.FAILED
        assert len(state.errors) == 1

    @pytest.mark.asyncio
    async def test_resume_from_checkpoint(self, state_manager, mock_agent_spawner):
        """Pipeline can resume from checkpoint."""
        from state.machine import PipelinePhase

        # Create and advance state
        state = state_manager.create_state(["PMA"], job_id="resume-test")
        state_manager.transition_phase(state, PipelinePhase.GATEKEEPER)
        state.add_to_queue("https://pma.org/members")
        state_manager.transition_phase(state, PipelinePhase.DISCOVERY)
        state_manager.checkpoint(state)

        # Simulate restart - load from disk
        loaded_state = state_manager.load_state("resume-test")

        assert loaded_state.current_phase == PipelinePhase.DISCOVERY
        assert len(loaded_state.crawl_queue) == 1

        # Continue from checkpoint
        state_manager.transition_phase(loaded_state, PipelinePhase.CLASSIFICATION)

        assert loaded_state.current_phase == PipelinePhase.CLASSIFICATION
