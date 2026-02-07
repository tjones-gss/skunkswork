"""
Pipeline State Machine
NAM Intelligence Pipeline

State management for pipeline execution with checkpoint/resume support.
Based on gss-research-engine/state_schema.json.

State Buckets:
- crawl_queue: URLs pending fetch
- visited_urls: URLs already fetched
- blocked_urls: URLs blocked by robots.txt or auth
- pages: Fetched page snapshots
- companies: Extracted company records
- events: Extracted event records
- participants: Extracted event participants
- competitor_signals: Detected competitor mentions
- canonical_entities: Resolved/deduplicated entities
- graph_edges: Relationship graph edges
- exports: Generated export files
- errors: Error records for debugging
"""

import json
import logging
import uuid
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class PipelinePhase(StrEnum):
    """Pipeline execution phases."""

    INIT = "INIT"
    GATEKEEPER = "GATEKEEPER"
    DISCOVERY = "DISCOVERY"
    CLASSIFICATION = "CLASSIFICATION"
    EXTRACTION = "EXTRACTION"
    ENRICHMENT = "ENRICHMENT"
    VALIDATION = "VALIDATION"
    RESOLUTION = "RESOLUTION"
    GRAPH = "GRAPH"
    EXPORT = "EXPORT"
    MONITOR = "MONITOR"
    DONE = "DONE"
    FAILED = "FAILED"


# Phase transitions
PHASE_TRANSITIONS = {
    PipelinePhase.INIT: [PipelinePhase.GATEKEEPER, PipelinePhase.FAILED],
    PipelinePhase.GATEKEEPER: [PipelinePhase.DISCOVERY, PipelinePhase.FAILED],
    PipelinePhase.DISCOVERY: [PipelinePhase.CLASSIFICATION, PipelinePhase.FAILED],
    PipelinePhase.CLASSIFICATION: [PipelinePhase.EXTRACTION, PipelinePhase.FAILED],
    PipelinePhase.EXTRACTION: [PipelinePhase.ENRICHMENT, PipelinePhase.FAILED],
    PipelinePhase.ENRICHMENT: [PipelinePhase.VALIDATION, PipelinePhase.FAILED],
    PipelinePhase.VALIDATION: [PipelinePhase.RESOLUTION, PipelinePhase.FAILED],
    PipelinePhase.RESOLUTION: [PipelinePhase.GRAPH, PipelinePhase.FAILED],
    PipelinePhase.GRAPH: [PipelinePhase.EXPORT, PipelinePhase.FAILED],
    PipelinePhase.EXPORT: [PipelinePhase.MONITOR, PipelinePhase.DONE, PipelinePhase.FAILED],
    PipelinePhase.MONITOR: [PipelinePhase.DONE, PipelinePhase.FAILED],
    PipelinePhase.DONE: [],
    PipelinePhase.FAILED: [],
}


class QueueItem(BaseModel):
    """Item in the crawl queue."""

    url: str
    priority: int = Field(default=0)
    depth: int = Field(default=0)
    source_url: str | None = None
    association: str | None = None
    page_type_hint: str | None = None
    added_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class PageSnapshot(BaseModel):
    """Snapshot of a fetched page."""

    url: str
    html_hash: str
    content_path: str  # Path to stored HTML file
    page_type: str | None = None
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    status_code: int = Field(default=200)


class ErrorRecord(BaseModel):
    """Record of an error during pipeline execution."""

    phase: str
    agent: str
    error_type: str
    error_message: str
    url: str | None = None
    context: dict = Field(default_factory=dict)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class PipelineState(BaseModel):
    """
    Complete pipeline state for checkpoint/resume.

    Tracks all data buckets and execution progress.
    """

    # Job identification
    job_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    association_codes: list[str] = Field(default_factory=list)

    # Current phase
    current_phase: PipelinePhase = Field(default=PipelinePhase.INIT)
    phase_started_at: datetime | None = None

    # Data buckets (from state_schema.json)
    crawl_queue: list[dict] = Field(default_factory=list)
    visited_urls: list[str] = Field(default_factory=list)
    blocked_urls: list[str] = Field(default_factory=list)
    pages: list[dict] = Field(default_factory=list)
    companies: list[dict] = Field(default_factory=list)
    events: list[dict] = Field(default_factory=list)
    participants: list[dict] = Field(default_factory=list)
    competitor_signals: list[dict] = Field(default_factory=list)
    canonical_entities: list[dict] = Field(default_factory=list)
    graph_edges: list[dict] = Field(default_factory=list)
    exports: list[dict] = Field(default_factory=list)
    errors: list[dict] = Field(default_factory=list)

    # Progress tracking
    total_urls_discovered: int = Field(default=0)
    total_pages_fetched: int = Field(default=0)
    total_companies_extracted: int = Field(default=0)
    total_events_extracted: int = Field(default=0)
    total_participants_extracted: int = Field(default=0)
    total_signals_detected: int = Field(default=0)
    total_entities_resolved: int = Field(default=0)

    # Timestamps
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None

    # Phase history
    phase_history: list[dict] = Field(default_factory=list)

    def transition_to(self, new_phase: PipelinePhase) -> bool:
        """
        Transition to a new pipeline phase.

        Returns True if transition is valid, False otherwise.
        """
        valid_transitions = PHASE_TRANSITIONS.get(self.current_phase, [])

        if new_phase not in valid_transitions:
            logger.warning(
                f"Invalid phase transition: {self.current_phase} -> {new_phase}. "
                f"Valid transitions: {valid_transitions}"
            )
            return False

        # Record phase history
        if self.phase_started_at:
            self.phase_history.append({
                "phase": self.current_phase,
                "started_at": self.phase_started_at.isoformat(),
                "ended_at": datetime.now(UTC).isoformat(),
                "stats": {
                    "urls_discovered": self.total_urls_discovered,
                    "pages_fetched": self.total_pages_fetched,
                    "companies": self.total_companies_extracted,
                    "events": self.total_events_extracted,
                }
            })

        self.current_phase = new_phase
        self.phase_started_at = datetime.now(UTC)
        self.updated_at = datetime.now(UTC)

        if new_phase == PipelinePhase.DONE:
            self.completed_at = datetime.now(UTC)

        logger.info(f"Pipeline transitioned to phase: {new_phase}")
        return True

    def add_to_queue(self, url: str, priority: int = 0, **kwargs):
        """Add URL to crawl queue."""
        if url in self.visited_urls or url in self.blocked_urls:
            return

        # Check if already in queue
        for item in self.crawl_queue:
            if item.get("url") == url:
                return

        self.crawl_queue.append({
            "url": url,
            "priority": priority,
            "added_at": datetime.now(UTC).isoformat(),
            **kwargs
        })
        self.total_urls_discovered += 1
        self.updated_at = datetime.now(UTC)

    def get_next_url(self) -> dict | None:
        """Get next URL from queue (highest priority first)."""
        if not self.crawl_queue:
            return None

        # Sort by priority (descending) and get first
        self.crawl_queue.sort(key=lambda x: x.get("priority", 0), reverse=True)
        return self.crawl_queue.pop(0)

    def mark_visited(self, url: str):
        """Mark URL as visited."""
        if url not in self.visited_urls:
            self.visited_urls.append(url)
            self.total_pages_fetched += 1
            self.updated_at = datetime.now(UTC)

    def mark_blocked(self, url: str, reason: str = None):
        """Mark URL as blocked."""
        if url not in self.blocked_urls:
            self.blocked_urls.append(url)
            self.updated_at = datetime.now(UTC)

    def add_page(self, page: dict):
        """Add fetched page snapshot."""
        self.pages.append(page)
        self.updated_at = datetime.now(UTC)

    def add_company(self, company: dict):
        """Add extracted company."""
        self.companies.append(company)
        self.total_companies_extracted += 1
        self.updated_at = datetime.now(UTC)

    def add_event(self, event: dict):
        """Add extracted event."""
        self.events.append(event)
        self.total_events_extracted += 1
        self.updated_at = datetime.now(UTC)

    def add_participant(self, participant: dict):
        """Add extracted participant."""
        self.participants.append(participant)
        self.total_participants_extracted += 1
        self.updated_at = datetime.now(UTC)

    def add_signal(self, signal: dict):
        """Add competitor signal."""
        self.competitor_signals.append(signal)
        self.total_signals_detected += 1
        self.updated_at = datetime.now(UTC)

    def add_canonical_entity(self, entity: dict):
        """Add resolved canonical entity."""
        self.canonical_entities.append(entity)
        self.total_entities_resolved += 1
        self.updated_at = datetime.now(UTC)

    def add_edge(self, edge: dict):
        """Add graph edge."""
        self.graph_edges.append(edge)
        self.updated_at = datetime.now(UTC)

    def add_export(self, export: dict):
        """Add export record."""
        self.exports.append(export)
        self.updated_at = datetime.now(UTC)

    def add_error(self, error: dict):
        """Add error record."""
        self.errors.append(error)
        self.updated_at = datetime.now(UTC)

    def get_summary(self) -> dict:
        """Get summary of current state."""
        return {
            "job_id": self.job_id,
            "associations": self.association_codes,
            "current_phase": self.current_phase,
            "queue_size": len(self.crawl_queue),
            "visited_urls": len(self.visited_urls),
            "blocked_urls": len(self.blocked_urls),
            "pages_fetched": len(self.pages),
            "companies_extracted": self.total_companies_extracted,
            "events_extracted": self.total_events_extracted,
            "participants_extracted": self.total_participants_extracted,
            "signals_detected": self.total_signals_detected,
            "entities_resolved": self.total_entities_resolved,
            "errors": len(self.errors),
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class StateManager:
    """
    Manages pipeline state persistence and recovery.

    Handles checkpointing and resumption of pipeline execution.
    """

    def __init__(self, state_dir: str = "data/.state"):
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    def _get_state_path(self, job_id: str) -> Path:
        """Get path to state file for a job."""
        return self.state_dir / f"{job_id}.state.json"

    def _get_checkpoint_path(self, job_id: str, phase: str) -> Path:
        """Get path to checkpoint file for a job/phase."""
        return self.state_dir / f"{job_id}.{phase}.checkpoint.json"

    def create_state(
        self,
        associations: list[str],
        job_id: str = None
    ) -> PipelineState:
        """Create new pipeline state."""
        state = PipelineState(
            job_id=job_id or str(uuid.uuid4()),
            association_codes=associations
        )

        self.save_state(state)
        logger.info(f"Created new pipeline state: {state.job_id}")

        return state

    def save_state(self, state: PipelineState):
        """Save state to disk."""
        path = self._get_state_path(state.job_id)

        with open(path, "w", encoding="utf-8") as f:
            json.dump(state.model_dump(mode="json"), f, indent=2, default=str)

        logger.debug(f"Saved state to {path}")

    def load_state(self, job_id: str) -> PipelineState | None:
        """Load state from disk."""
        path = self._get_state_path(job_id)

        if not path.exists():
            logger.warning(f"State file not found: {path}")
            return None

        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        state = PipelineState(**data)
        logger.info(f"Loaded state for job {job_id}, phase: {state.current_phase}")

        return state

    def checkpoint(self, state: PipelineState):
        """
        Create checkpoint at current phase.

        Saves both full state and phase-specific checkpoint.
        """
        # Save full state
        self.save_state(state)

        # Save phase checkpoint
        checkpoint_path = self._get_checkpoint_path(
            state.job_id,
            state.current_phase.value
        )

        checkpoint = {
            "job_id": state.job_id,
            "phase": state.current_phase.value,
            "timestamp": datetime.now(UTC).isoformat(),
            "summary": state.get_summary()
        }

        with open(checkpoint_path, "w", encoding="utf-8") as f:
            json.dump(checkpoint, f, indent=2)

        logger.info(
            f"Checkpoint created for job {state.job_id} "
            f"at phase {state.current_phase}"
        )

    def get_latest_checkpoint(self, job_id: str) -> dict | None:
        """Get the most recent checkpoint for a job."""
        checkpoints = list(self.state_dir.glob(f"{job_id}.*.checkpoint.json"))

        if not checkpoints:
            return None

        # Sort by modification time
        latest = max(checkpoints, key=lambda p: p.stat().st_mtime)

        with open(latest, encoding="utf-8") as f:
            return json.load(f)

    def list_jobs(self, include_completed: bool = False) -> list[dict]:
        """List all pipeline jobs."""
        jobs = []

        for path in self.state_dir.glob("*.state.json"):
            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)

                job_info = {
                    "job_id": data["job_id"],
                    "associations": data.get("association_codes", []),
                    "phase": data["current_phase"],
                    "created_at": data["created_at"],
                    "updated_at": data["updated_at"],
                    "completed_at": data.get("completed_at"),
                }

                if include_completed or not data.get("completed_at"):
                    jobs.append(job_info)

            except Exception as e:
                logger.warning(f"Failed to read state file {path}: {e}")

        return sorted(jobs, key=lambda x: x["updated_at"], reverse=True)

    def delete_job(self, job_id: str):
        """Delete all state files for a job."""
        # Delete main state
        state_path = self._get_state_path(job_id)
        if state_path.exists():
            state_path.unlink()

        # Delete checkpoints
        for checkpoint in self.state_dir.glob(f"{job_id}.*.checkpoint.json"):
            checkpoint.unlink()

        logger.info(f"Deleted state for job {job_id}")

    def transition_phase(
        self,
        state: PipelineState,
        new_phase: PipelinePhase
    ) -> bool:
        """
        Transition to new phase with checkpoint.

        Returns True if successful.
        """
        if state.transition_to(new_phase):
            self.checkpoint(state)
            return True
        return False
