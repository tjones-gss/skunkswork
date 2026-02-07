"""
Pipeline Integration Tests
NAM Intelligence Pipeline

Integration tests for pipeline phase transitions, state management, and orchestration.
"""

from datetime import UTC, datetime

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
        from state.machine import PipelinePhase

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
        from state.machine import PHASE_TRANSITIONS, PipelinePhase

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
        from state.machine import PHASE_TRANSITIONS, PipelinePhase

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
        import time

        from state.machine import PipelinePhase

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

        state_manager.create_state(["PMA"], job_id="older-job")
        time.sleep(0.01)
        state_manager.create_state(["NEMA"], job_id="newer-job")

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



# =============================================================================
# TEST: Partial-Phase Resume (P3-T02)
# =============================================================================


class TestPartialPhaseResume:
    """Tests for intra-phase progress tracking and crash-resume."""

    def test_phase_progress_defaults_empty(self, fresh_pipeline_state):
        """phase_progress is an empty dict by default."""
        assert fresh_pipeline_state.phase_progress == {}

    def test_update_phase_progress_sets_cursor(self, fresh_pipeline_state):
        """update_phase_progress() stores cursor data."""
        fresh_pipeline_state.update_phase_progress(cursor=50, total=200)

        assert fresh_pipeline_state.phase_progress == {
            "cursor": 50,
            "total": 200,
        }

    def test_update_phase_progress_merges(self, fresh_pipeline_state):
        """Successive calls merge, not replace."""
        fresh_pipeline_state.update_phase_progress(cursor=10)
        fresh_pipeline_state.update_phase_progress(last_url="https://example.com")

        assert fresh_pipeline_state.phase_progress == {
            "cursor": 10,
            "last_url": "https://example.com",
        }

    def test_update_phase_progress_overwrites_existing_key(self, fresh_pipeline_state):
        """A key already present is overwritten by a new call."""
        fresh_pipeline_state.update_phase_progress(cursor=10)
        fresh_pipeline_state.update_phase_progress(cursor=20)

        assert fresh_pipeline_state.phase_progress["cursor"] == 20

    def test_clear_phase_progress(self, fresh_pipeline_state):
        """clear_phase_progress() resets to empty dict."""
        fresh_pipeline_state.update_phase_progress(cursor=99)
        fresh_pipeline_state.clear_phase_progress()

        assert fresh_pipeline_state.phase_progress == {}

    def test_transition_resets_phase_progress(self, fresh_pipeline_state):
        """Transitioning to a new phase clears phase_progress."""
        from state.machine import PipelinePhase

        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)
        fresh_pipeline_state.update_phase_progress(cursor=42, total=100)

        # Progress should exist
        assert fresh_pipeline_state.phase_progress["cursor"] == 42

        # Transition clears it
        fresh_pipeline_state.transition_to(PipelinePhase.DISCOVERY)
        assert fresh_pipeline_state.phase_progress == {}

    def test_get_summary_includes_phase_progress(self, fresh_pipeline_state):
        """get_summary() includes phase_progress."""
        fresh_pipeline_state.update_phase_progress(cursor=5, batch=2)
        summary = fresh_pipeline_state.get_summary()

        assert "phase_progress" in summary
        assert summary["phase_progress"] == {"cursor": 5, "batch": 2}

    def test_checkpoint_persists_phase_progress(self, state_manager, fresh_pipeline_state):
        """Checkpoint file contains phase_progress."""
        from state.machine import PipelinePhase

        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)
        fresh_pipeline_state.transition_to(PipelinePhase.DISCOVERY)
        fresh_pipeline_state.update_phase_progress(cursor=150, total=500)
        state_manager.checkpoint(fresh_pipeline_state)

        # Read checkpoint file directly
        cp = state_manager.get_latest_checkpoint(fresh_pipeline_state.job_id)
        assert cp is not None
        assert cp["phase_progress"] == {"cursor": 150, "total": 500}

    def test_save_load_preserves_phase_progress(self, state_manager, fresh_pipeline_state):
        """phase_progress survives a save/load round-trip."""
        from state.machine import PipelinePhase

        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)
        fresh_pipeline_state.update_phase_progress(
            cursor=300, total=1200, last_url="https://pma.org/page/30"
        )
        state_manager.save_state(fresh_pipeline_state)

        loaded = state_manager.load_state(fresh_pipeline_state.job_id)

        assert loaded.phase_progress == {
            "cursor": 300,
            "total": 1200,
            "last_url": "https://pma.org/page/30",
        }

    def test_crash_resume_scenario(self, state_manager):
        """Simulate crash mid-phase → reload → verify resume from cursor."""
        from state.machine import PipelinePhase

        # --- simulate running pipeline ---
        state = state_manager.create_state(["PMA"], job_id="crash-resume-test")
        state_manager.transition_phase(state, PipelinePhase.GATEKEEPER)
        state_manager.transition_phase(state, PipelinePhase.DISCOVERY)

        # Add some URLs and mark partial progress
        for i in range(100):
            state.add_to_queue(f"https://pma.org/member/{i}")

        # Process first 45 URLs, updating cursor each batch
        state.update_phase_progress(cursor=45, total=100, batch_number=5)
        state_manager.checkpoint(state)

        # --- simulate crash & restart ---
        reloaded = state_manager.load_state("crash-resume-test")

        # Verify we know where we left off
        assert reloaded.current_phase == PipelinePhase.DISCOVERY
        assert reloaded.phase_progress["cursor"] == 45
        assert reloaded.phase_progress["total"] == 100
        assert reloaded.phase_progress["batch_number"] == 5

        # Continue from cursor=45 (not from 0)
        reloaded.update_phase_progress(cursor=100, total=100)
        state_manager.checkpoint(reloaded)

        # Verify final state
        final = state_manager.load_state("crash-resume-test")
        assert final.phase_progress["cursor"] == 100

    def test_update_phase_progress_updates_timestamp(self, fresh_pipeline_state):
        """update_phase_progress() bumps updated_at."""
        before = fresh_pipeline_state.updated_at
        import time
        time.sleep(0.01)
        fresh_pipeline_state.update_phase_progress(cursor=1)
        assert fresh_pipeline_state.updated_at > before

    def test_clear_phase_progress_updates_timestamp(self, fresh_pipeline_state):
        """clear_phase_progress() bumps updated_at."""
        fresh_pipeline_state.update_phase_progress(cursor=1)
        before = fresh_pipeline_state.updated_at
        import time
        time.sleep(0.01)
        fresh_pipeline_state.clear_phase_progress()
        assert fresh_pipeline_state.updated_at > before



# =============================================================================
# TEST: End-to-End Pipeline With Mocked APIs (P3-T03)
# =============================================================================


class TestEndToEndWithMockedAPIs:
    """
    E2E test: full pipeline discovery → extraction → enrichment →
    validation → export with all external APIs mocked.

    WBS P3-T03 acceptance criteria:
      ≥1 company extracted, quality_score > 0, no unhandled exceptions,
      exports written, runs in CI without real API keys.
    """

    @pytest.fixture
    def e2e_pipeline(self, tmp_path, monkeypatch):
        """Create orchestrator wired to mock spawner for full-pipeline E2E."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from state.machine import PipelinePhase, StateManager

        monkeypatch.chdir(tmp_path)
        call_log: list[tuple[str, dict]] = []

        # -- mock spawn side-effect dispatching by agent type ----------------
        async def mock_spawn(agent_type, task):
            call_log.append((agent_type, dict(task) if isinstance(task, dict) else task))

            if agent_type == "discovery.access_gatekeeper":
                return {"success": True, "is_allowed": True}

            if agent_type == "discovery.site_mapper":
                url = task.get("base_url", "")
                if "/member/" not in url:
                    return {"success": True, "directory_url": f"{url}/directory"}
                return {"success": True}

            if agent_type == "discovery.link_crawler":
                return {
                    "success": True,
                    "member_urls": [
                        "https://www.pma.org/member/acme",
                        "https://www.pma.org/member/beta",
                    ],
                }

            if agent_type == "discovery.page_classifier":
                return {"success": True, "page_type": "MEMBER_DETAIL"}

            if agent_type == "extraction.html_parser":
                slug = task.get("url", "").rstrip("/").split("/")[-1]
                return {
                    "success": True,
                    "records": [{
                        "company_name": f"{slug.title()} Manufacturing",
                        "website": f"https://{slug}-mfg.com",
                        "domain": f"{slug}-mfg.com",
                        "city": "Cleveland", "state": "OH",
                        "country": "United States",
                        "associations": ["PMA"],
                    }],
                }

            if agent_type == "enrichment.firmographic":
                recs = [dict(r) for r in task.get("records", [])]
                for r in recs:
                    r.update(employee_count_min=50, employee_count_max=200,
                             revenue_min_usd=5_000_000, naics_code="332119")
                return {"success": True, "records": recs}

            if agent_type == "enrichment.tech_stack":
                recs = [dict(r) for r in task.get("records", [])]
                for r in recs:
                    r.update(tech_stack=["SAP", "Salesforce"], erp_system="SAP")
                return {"success": True, "records": recs}

            if agent_type == "enrichment.contact_finder":
                recs = [dict(r) for r in task.get("records", [])]
                for r in recs:
                    r["contacts"] = [{"name": "J Doe", "title": "VP Ops",
                                      "email": "jdoe@example.com"}]
                return {"success": True, "records": recs}

            if agent_type == "validation.dedupe":
                return {"success": True, "records": task.get("records", [])}

            if agent_type == "validation.crossref":
                recs = [dict(r) for r in task.get("records", [])]
                for r in recs:
                    r["crossref_verified"] = True
                return {"success": True, "records": recs}

            if agent_type == "validation.scorer":
                recs = [dict(r) for r in task.get("records", [])]
                for r in recs:
                    r["quality_score"] = 85
                return {"success": True, "records": recs}

            if agent_type == "validation.entity_resolver":
                return {"success": True,
                        "canonical_entities": task.get("records", [])}

            if agent_type == "intelligence.competitor_signal_miner":
                return {"success": True,
                        "signals": [{"competitor": "Epicor",
                                     "signal_type": "mention"}]}

            if agent_type == "intelligence.relationship_graph_builder":
                return {"success": True, "edges_created": 5}

            if agent_type == "export.export_activation":
                return {"success": True,
                        "export_path": f"data/exports/{task.get('export_type')}.csv",
                        "records_exported": len(
                            task.get("records", task.get("companies", [])))}

            if agent_type == "monitoring.source_monitor":
                return {"success": True}

            return {"success": False, "error": f"Unknown agent: {agent_type}"}

        # -- build orchestrator under patches --------------------------------
        with patch("agents.base.Config") as mc, \
             patch("agents.base.StructuredLogger"), \
             patch("agents.base.AsyncHTTPClient") as mock_http, \
             patch("agents.base.RateLimiter"), \
             patch("agents.base.DeadLetterQueue"):

            mc.return_value.load.side_effect = lambda name: (
                {"associations": {
                    "PMA": {"url": "https://www.pma.org", "priority": "high"}
                }}
                if name == "associations" else {}
            )
            mock_http.return_value.close = AsyncMock()

            from agents.orchestrator import OrchestratorAgent
            orch = OrchestratorAgent(
                agent_type="orchestrator",
                mode="full",
                associations=["PMA"],
                dry_run=False,
            )

        orch.log = MagicMock()
        orch.state_manager = StateManager(state_dir=str(tmp_path / "state"))

        mock_spawner = MagicMock()
        mock_spawner.spawn = AsyncMock(side_effect=mock_spawn)
        orch.spawner = mock_spawner

        # Patch _execute_phase: inject URLs before EXTRACTION so that the
        # extraction phase has work to do (discovery consumes the queue).
        original_execute = orch._execute_phase

        async def _patched_execute(phase):
            if phase == PipelinePhase.EXTRACTION and not orch.state.crawl_queue:
                for slug in ("acme-ext", "beta-ext"):
                    orch.state.crawl_queue.append({
                        "url": f"https://www.pma.org/member/{slug}",
                        "association": "PMA",
                        "page_type_hint": "MEMBER_DETAIL",
                        "priority": 5,
                    })
            return await original_execute(phase)

        orch._execute_phase = _patched_execute
        return orch, call_log

    # -- test methods --------------------------------------------------------

    @pytest.mark.asyncio
    async def test_full_pipeline_happy_path(self, e2e_pipeline):
        """Pipeline runs INIT→…→DONE, extracts ≥1 company with score > 0."""
        orch, _ = e2e_pipeline
        result = await orch.run({"mode": "full", "associations": ["PMA"]})

        assert result["success"] is True
        assert result["final_phase"] == "DONE"
        assert result["totals"]["companies_extracted"] >= 1
        assert len(result["errors"]) == 0

        for company in orch.state.companies:
            assert company.get("quality_score", 0) > 0, (
                f"{company.get('company_name')} has no quality_score"
            )

    @pytest.mark.asyncio
    async def test_all_phase_agents_called(self, e2e_pipeline):
        """Every expected agent type is invoked at least once."""
        orch, call_log = e2e_pipeline
        await orch.run({"mode": "full", "associations": ["PMA"]})

        called = {a for a, _ in call_log}
        expected = {
            "discovery.access_gatekeeper", "discovery.site_mapper",
            "discovery.link_crawler", "extraction.html_parser",
            "enrichment.firmographic", "enrichment.tech_stack",
            "enrichment.contact_finder", "validation.dedupe",
            "validation.crossref", "validation.scorer",
            "validation.entity_resolver",
            "intelligence.competitor_signal_miner",
            "intelligence.relationship_graph_builder",
            "export.export_activation", "monitoring.source_monitor",
        }
        missing = expected - called
        assert not missing, f"Agents never called: {missing}"

    @pytest.mark.asyncio
    async def test_exports_written_to_disk(self, e2e_pipeline):
        """Result files and company JSONL are persisted."""
        from pathlib import Path

        orch, _ = e2e_pipeline
        await orch.run({"mode": "full", "associations": ["PMA"]})

        job_dir = Path(f"data/validated/{orch.state.job_id}")
        assert (job_dir / "pipeline_result.json").exists()
        assert (job_dir / "companies.jsonl").exists()
        assert len(orch.state.exports) >= 1

    @pytest.mark.asyncio
    async def test_runs_without_api_keys(self, e2e_pipeline, monkeypatch):
        """Pipeline succeeds even when all API-key env vars are unset."""
        for key in ("CLEARBIT_API_KEY", "APOLLO_API_KEY", "BUILTWITH_API_KEY",
                     "HUNTER_API_KEY", "ZOOMINFO_API_KEY", "GOOGLE_PLACES_API_KEY"):
            monkeypatch.delenv(key, raising=False)

        orch, _ = e2e_pipeline
        result = await orch.run({"mode": "full", "associations": ["PMA"]})
        assert result["success"] is True

    @pytest.mark.skip(reason="P2-T02 circuit breaker not yet landed")
    @pytest.mark.asyncio
    async def test_circuit_breaker_under_api_failure(self, e2e_pipeline):
        """Circuit breaker trips under simulated API failure."""
        pass
