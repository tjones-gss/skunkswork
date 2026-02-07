"""
State Machine Unit Tests
NAM Intelligence Pipeline

Unit tests for state/machine.py - Pipeline state and state manager.
"""

import json
from datetime import datetime, UTC
from pathlib import Path

import pytest


# =============================================================================
# TEST: PipelinePhase Enum
# =============================================================================


class TestPipelinePhaseEnum:
    """Tests for PipelinePhase enum."""

    def test_all_phases_defined(self):
        """All expected phases are defined."""
        from state.machine import PipelinePhase

        expected_phases = [
            "INIT", "GATEKEEPER", "DISCOVERY", "CLASSIFICATION",
            "EXTRACTION", "ENRICHMENT", "VALIDATION", "RESOLUTION",
            "GRAPH", "EXPORT", "MONITOR", "DONE", "FAILED"
        ]

        for phase_name in expected_phases:
            assert hasattr(PipelinePhase, phase_name)

    def test_phase_is_string_enum(self):
        """PipelinePhase values are strings."""
        from state.machine import PipelinePhase

        assert PipelinePhase.INIT.value == "INIT"
        assert PipelinePhase.DONE.value == "DONE"
        assert PipelinePhase.FAILED.value == "FAILED"

    def test_phase_string_behavior(self):
        """PipelinePhase behaves as string."""
        from state.machine import PipelinePhase

        # Can use in string operations
        phase = PipelinePhase.DISCOVERY
        # str() includes class name for StrEnum in Python 3.12+
        assert "DISCOVERY" in str(phase)
        # Value comparison works
        assert phase.value == "DISCOVERY"
        # Can compare with string value
        assert phase == PipelinePhase.DISCOVERY

    def test_phase_count(self):
        """Correct number of phases defined."""
        from state.machine import PipelinePhase

        # 13 phases: INIT through FAILED
        assert len(PipelinePhase) == 13


# =============================================================================
# TEST: QueueItem Model
# =============================================================================


class TestQueueItem:
    """Tests for QueueItem model."""

    def test_queue_item_defaults(self):
        """QueueItem has correct default values."""
        from state.machine import QueueItem

        item = QueueItem(url="https://test.com")

        assert item.url == "https://test.com"
        assert item.priority == 0
        assert item.depth == 0
        assert item.source_url is None
        assert item.association is None
        assert item.page_type_hint is None
        assert isinstance(item.added_at, datetime)

    def test_queue_item_all_fields(self):
        """QueueItem accepts all fields."""
        from state.machine import QueueItem

        item = QueueItem(
            url="https://test.com/page",
            priority=5,
            depth=2,
            source_url="https://test.com",
            association="PMA",
            page_type_hint="MEMBER_DIRECTORY"
        )

        assert item.priority == 5
        assert item.depth == 2
        assert item.source_url == "https://test.com"
        assert item.association == "PMA"
        assert item.page_type_hint == "MEMBER_DIRECTORY"

    def test_queue_item_serialization(self):
        """QueueItem can be serialized to dict."""
        from state.machine import QueueItem

        item = QueueItem(url="https://test.com", priority=1)
        data = item.model_dump()

        assert data["url"] == "https://test.com"
        assert data["priority"] == 1
        assert "added_at" in data


# =============================================================================
# TEST: PageSnapshot Model
# =============================================================================


class TestPageSnapshot:
    """Tests for PageSnapshot model."""

    def test_page_snapshot_creation(self):
        """PageSnapshot can be created."""
        from state.machine import PageSnapshot

        snapshot = PageSnapshot(
            url="https://test.com/members",
            html_hash="abc123",
            content_path="data/raw/pages/test.html"
        )

        assert snapshot.url == "https://test.com/members"
        assert snapshot.html_hash == "abc123"
        assert snapshot.content_path == "data/raw/pages/test.html"
        assert snapshot.page_type is None
        assert snapshot.status_code == 200
        assert isinstance(snapshot.fetched_at, datetime)

    def test_page_snapshot_all_fields(self):
        """PageSnapshot accepts all fields."""
        from state.machine import PageSnapshot

        snapshot = PageSnapshot(
            url="https://test.com",
            html_hash="def456",
            content_path="test.html",
            page_type="MEMBER_DETAIL",
            status_code=200
        )

        assert snapshot.page_type == "MEMBER_DETAIL"
        assert snapshot.status_code == 200


# =============================================================================
# TEST: ErrorRecord Model
# =============================================================================


class TestErrorRecord:
    """Tests for ErrorRecord model."""

    def test_error_record_creation(self):
        """ErrorRecord can be created."""
        from state.machine import ErrorRecord

        error = ErrorRecord(
            phase="EXTRACTION",
            agent="html_parser",
            error_type="ParseError",
            error_message="Failed to parse HTML"
        )

        assert error.phase == "EXTRACTION"
        assert error.agent == "html_parser"
        assert error.error_type == "ParseError"
        assert error.error_message == "Failed to parse HTML"
        assert error.url is None
        assert error.context == {}
        assert isinstance(error.occurred_at, datetime)

    def test_error_record_with_context(self):
        """ErrorRecord accepts context dict."""
        from state.machine import ErrorRecord

        error = ErrorRecord(
            phase="DISCOVERY",
            agent="link_crawler",
            error_type="HTTPError",
            error_message="Connection refused",
            url="https://test.com",
            context={"status_code": 503, "retry_count": 3}
        )

        assert error.url == "https://test.com"
        assert error.context["status_code"] == 503
        assert error.context["retry_count"] == 3


# =============================================================================
# TEST: PipelineState Model
# =============================================================================


class TestPipelineStateModel:
    """Tests for PipelineState model."""

    def test_pipeline_state_defaults(self):
        """PipelineState has correct default values."""
        from state.machine import PipelineState, PipelinePhase

        state = PipelineState()

        assert state.job_id is not None
        assert len(state.job_id) == 36  # UUID format
        assert state.association_codes == []
        assert state.current_phase == PipelinePhase.INIT
        assert state.crawl_queue == []
        assert state.visited_urls == []
        assert state.blocked_urls == []
        assert state.pages == []
        assert state.companies == []
        assert state.events == []
        assert state.participants == []
        assert state.competitor_signals == []
        assert state.canonical_entities == []
        assert state.graph_edges == []
        assert state.exports == []
        assert state.errors == []
        assert state.total_urls_discovered == 0
        assert state.total_pages_fetched == 0
        assert state.total_companies_extracted == 0
        assert state.phase_history == []

    def test_pipeline_state_with_associations(self):
        """PipelineState accepts association codes."""
        from state.machine import PipelineState

        state = PipelineState(association_codes=["PMA", "NEMA", "SOCMA"])

        assert state.association_codes == ["PMA", "NEMA", "SOCMA"]

    def test_pipeline_state_custom_job_id(self):
        """PipelineState accepts custom job_id."""
        from state.machine import PipelineState

        state = PipelineState(job_id="custom-job-123")

        assert state.job_id == "custom-job-123"

    def test_pipeline_state_serialization(self):
        """PipelineState can be serialized to dict."""
        from state.machine import PipelineState

        state = PipelineState(
            job_id="test-job",
            association_codes=["PMA"]
        )
        state.add_to_queue("https://test.com")

        data = state.model_dump(mode="json")

        assert data["job_id"] == "test-job"
        assert data["association_codes"] == ["PMA"]
        assert len(data["crawl_queue"]) == 1
        assert data["current_phase"] == "INIT"

    def test_pipeline_state_deserialization(self):
        """PipelineState can be deserialized from dict."""
        from state.machine import PipelineState, PipelinePhase

        data = {
            "job_id": "test-job-456",
            "association_codes": ["NEMA"],
            "current_phase": "DISCOVERY",
            "crawl_queue": [{"url": "https://test.com", "priority": 1}],
            "visited_urls": ["https://visited.com"],
            "blocked_urls": [],
            "pages": [],
            "companies": [],
            "events": [],
            "participants": [],
            "competitor_signals": [],
            "canonical_entities": [],
            "graph_edges": [],
            "exports": [],
            "errors": [],
            "total_urls_discovered": 1,
            "total_pages_fetched": 1,
            "total_companies_extracted": 0,
            "total_events_extracted": 0,
            "total_participants_extracted": 0,
            "total_signals_detected": 0,
            "total_entities_resolved": 0,
            "phase_history": [],
            "created_at": "2024-01-15T10:00:00",
            "updated_at": "2024-01-15T10:30:00"
        }

        state = PipelineState(**data)

        assert state.job_id == "test-job-456"
        assert state.association_codes == ["NEMA"]
        assert state.current_phase == PipelinePhase.DISCOVERY
        assert len(state.crawl_queue) == 1
        assert len(state.visited_urls) == 1

    def test_pipeline_state_timestamps(self):
        """PipelineState has auto-generated timestamps."""
        from state.machine import PipelineState

        state = PipelineState()

        assert isinstance(state.created_at, datetime)
        assert isinstance(state.updated_at, datetime)
        assert state.completed_at is None


# =============================================================================
# TEST: StateManager File Operations
# =============================================================================


class TestStateManagerFileOperations:
    """Tests for StateManager file operations."""

    def test_state_manager_creates_directory(self, tmp_path):
        """StateManager creates state directory if not exists."""
        from state.machine import StateManager

        state_dir = tmp_path / "new_state_dir"
        assert not state_dir.exists()

        manager = StateManager(state_dir=str(state_dir))

        assert state_dir.exists()
        assert state_dir.is_dir()

    def test_state_path_format(self, state_manager):
        """State file path has correct format."""
        path = state_manager._get_state_path("job-123")

        assert path.name == "job-123.state.json"
        assert path.parent == state_manager.state_dir

    def test_checkpoint_path_format(self, state_manager):
        """Checkpoint file path has correct format."""
        path = state_manager._get_checkpoint_path("job-123", "DISCOVERY")

        assert path.name == "job-123.DISCOVERY.checkpoint.json"
        assert path.parent == state_manager.state_dir

    def test_save_state_creates_json_file(self, state_manager, fresh_pipeline_state):
        """save_state creates valid JSON file."""
        state_manager.save_state(fresh_pipeline_state)

        state_path = state_manager._get_state_path(fresh_pipeline_state.job_id)
        assert state_path.exists()

        with open(state_path) as f:
            data = json.load(f)

        assert data["job_id"] == fresh_pipeline_state.job_id
        assert data["association_codes"] == fresh_pipeline_state.association_codes

    def test_load_state_reads_file(self, state_manager, fresh_pipeline_state):
        """load_state reads state from file."""
        state_manager.save_state(fresh_pipeline_state)

        loaded = state_manager.load_state(fresh_pipeline_state.job_id)

        assert loaded is not None
        assert loaded.job_id == fresh_pipeline_state.job_id
        assert loaded.association_codes == fresh_pipeline_state.association_codes

    def test_load_state_missing_file_returns_none(self, state_manager):
        """load_state returns None for non-existent job."""
        loaded = state_manager.load_state("nonexistent-job")

        assert loaded is None

    def test_create_state_generates_job_id(self, state_manager):
        """create_state generates job_id if not provided."""
        state = state_manager.create_state(associations=["PMA"])

        assert state.job_id is not None
        assert len(state.job_id) == 36

    def test_create_state_uses_provided_job_id(self, state_manager):
        """create_state uses provided job_id."""
        state = state_manager.create_state(
            associations=["NEMA"],
            job_id="custom-id-789"
        )

        assert state.job_id == "custom-id-789"

    def test_create_state_saves_to_disk(self, state_manager):
        """create_state saves state to disk."""
        state = state_manager.create_state(associations=["PMA"])

        state_path = state_manager._get_state_path(state.job_id)
        assert state_path.exists()

    def test_checkpoint_creates_files(self, state_manager, fresh_pipeline_state):
        """checkpoint creates state and checkpoint files."""
        fresh_pipeline_state.transition_to(fresh_pipeline_state.current_phase)
        state_manager.checkpoint(fresh_pipeline_state)

        state_path = state_manager._get_state_path(fresh_pipeline_state.job_id)
        checkpoint_path = state_manager._get_checkpoint_path(
            fresh_pipeline_state.job_id,
            fresh_pipeline_state.current_phase.value
        )

        assert state_path.exists()
        assert checkpoint_path.exists()

    def test_checkpoint_file_contents(self, state_manager, fresh_pipeline_state):
        """checkpoint file contains expected data."""
        from state.machine import PipelinePhase

        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)
        state_manager.checkpoint(fresh_pipeline_state)

        checkpoint_path = state_manager._get_checkpoint_path(
            fresh_pipeline_state.job_id,
            "GATEKEEPER"
        )

        with open(checkpoint_path) as f:
            data = json.load(f)

        assert data["job_id"] == fresh_pipeline_state.job_id
        assert data["phase"] == "GATEKEEPER"
        assert "timestamp" in data
        assert "summary" in data

    def test_get_latest_checkpoint(self, state_manager, fresh_pipeline_state):
        """get_latest_checkpoint returns most recent checkpoint."""
        from state.machine import PipelinePhase
        import time

        # Create checkpoints
        fresh_pipeline_state.transition_to(PipelinePhase.GATEKEEPER)
        state_manager.checkpoint(fresh_pipeline_state)

        time.sleep(0.01)  # Ensure different timestamps

        fresh_pipeline_state.transition_to(PipelinePhase.DISCOVERY)
        state_manager.checkpoint(fresh_pipeline_state)

        latest = state_manager.get_latest_checkpoint(fresh_pipeline_state.job_id)

        assert latest is not None
        assert latest["phase"] == "DISCOVERY"

    def test_get_latest_checkpoint_no_checkpoints(self, state_manager):
        """get_latest_checkpoint returns None if no checkpoints."""
        latest = state_manager.get_latest_checkpoint("nonexistent-job")

        assert latest is None

    def test_list_jobs_empty(self, state_manager):
        """list_jobs returns empty list when no jobs."""
        jobs = state_manager.list_jobs()

        assert jobs == []

    def test_list_jobs_returns_jobs(self, state_manager):
        """list_jobs returns created jobs."""
        state_manager.create_state(["PMA"], job_id="job-1")
        state_manager.create_state(["NEMA"], job_id="job-2")

        jobs = state_manager.list_jobs()

        assert len(jobs) == 2
        job_ids = [j["job_id"] for j in jobs]
        assert "job-1" in job_ids
        assert "job-2" in job_ids

    def test_list_jobs_excludes_completed(self, state_manager):
        """list_jobs excludes completed jobs by default."""
        from state.machine import PipelinePhase

        state1 = state_manager.create_state(["PMA"], job_id="job-active")
        state2 = state_manager.create_state(["NEMA"], job_id="job-done")

        # Complete one job
        state2.completed_at = datetime.now(UTC)
        state_manager.save_state(state2)

        jobs = state_manager.list_jobs(include_completed=False)

        assert len(jobs) == 1
        assert jobs[0]["job_id"] == "job-active"

    def test_list_jobs_includes_completed_when_requested(self, state_manager):
        """list_jobs includes completed jobs when requested."""
        state1 = state_manager.create_state(["PMA"], job_id="job-active")
        state2 = state_manager.create_state(["NEMA"], job_id="job-done")

        state2.completed_at = datetime.now(UTC)
        state_manager.save_state(state2)

        jobs = state_manager.list_jobs(include_completed=True)

        assert len(jobs) == 2

    def test_delete_job_removes_files(self, state_manager):
        """delete_job removes state and checkpoint files."""
        from state.machine import PipelinePhase

        state = state_manager.create_state(["PMA"], job_id="job-to-delete")
        state.transition_to(PipelinePhase.GATEKEEPER)
        state_manager.checkpoint(state)

        state_path = state_manager._get_state_path("job-to-delete")
        assert state_path.exists()

        state_manager.delete_job("job-to-delete")

        assert not state_path.exists()

        # Checkpoints also deleted
        checkpoints = list(state_manager.state_dir.glob("job-to-delete.*.checkpoint.json"))
        assert len(checkpoints) == 0

    def test_delete_job_nonexistent_ok(self, state_manager):
        """delete_job does not raise for non-existent job."""
        # Should not raise
        state_manager.delete_job("nonexistent-job")

    def test_transition_phase_with_checkpoint(self, state_manager, fresh_pipeline_state):
        """transition_phase creates checkpoint on success."""
        from state.machine import PipelinePhase

        result = state_manager.transition_phase(
            fresh_pipeline_state,
            PipelinePhase.GATEKEEPER
        )

        assert result is True
        assert fresh_pipeline_state.current_phase == PipelinePhase.GATEKEEPER

        checkpoint_path = state_manager._get_checkpoint_path(
            fresh_pipeline_state.job_id,
            "GATEKEEPER"
        )
        assert checkpoint_path.exists()

    def test_transition_phase_invalid_returns_false(self, state_manager, fresh_pipeline_state):
        """transition_phase returns False for invalid transition."""
        from state.machine import PipelinePhase

        # INIT cannot go directly to EXTRACTION
        result = state_manager.transition_phase(
            fresh_pipeline_state,
            PipelinePhase.EXTRACTION
        )

        assert result is False
        assert fresh_pipeline_state.current_phase == PipelinePhase.INIT
