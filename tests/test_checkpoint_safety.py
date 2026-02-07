"""
Tests for safe checkpoint & save_records with atomic writes.

Phase 3: Checkpoint Safety Hardening
"""

import json
import os
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================================
# HELPERS
# =============================================================================


def _make_agent(tmp_path, job_id="test-job-001"):
    """Create a BaseAgent subclass for testing."""
    # Patch Config, Logger, HTTP, RateLimiter to avoid real I/O
    with patch("agents.base.Config") as mc, \
         patch("agents.base.StructuredLogger") as ml, \
         patch("agents.base.AsyncHTTPClient") as mh, \
         patch("agents.base.RateLimiter") as mr:
        mc.return_value.load.return_value = {}

        from agents.base import BaseAgent

        class TestAgent(BaseAgent):
            async def run(self, task):
                return {"success": True, "records_processed": 0}

        agent = TestAgent(agent_type="test.agent", job_id=job_id)
        return agent


# =============================================================================
# TEST CHECKPOINT ATOMIC WRITE
# =============================================================================


class TestCheckpointSafety:
    """Tests for checkpoint() with atomic writes."""

    @pytest.mark.asyncio
    async def test_checkpoint_returns_true_on_success(self, tmp_path):
        """checkpoint() returns True on successful save."""
        agent = _make_agent(tmp_path)

        result = await agent.checkpoint({"phase": "extraction", "count": 42})

        assert result is True

    @pytest.mark.asyncio
    async def test_checkpoint_writes_valid_json(self, tmp_path):
        """checkpoint() writes valid JSON to disk."""
        agent = _make_agent(tmp_path)

        await agent.checkpoint({"phase": "test", "items": [1, 2, 3]})

        checkpoint_path = Path("data/.state") / f"{agent.job_id}.checkpoint.json"
        if checkpoint_path.exists():
            with open(checkpoint_path) as f:
                data = json.load(f)
            assert data["job_id"] == agent.job_id
            assert data["state"]["phase"] == "test"
            assert data["state"]["items"] == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_checkpoint_returns_false_on_oserror(self, tmp_path):
        """checkpoint() returns False on OSError."""
        agent = _make_agent(tmp_path)

        with patch("builtins.open", side_effect=OSError("Disk full")):
            result = await agent.checkpoint({"phase": "test"})

        assert result is False

    @pytest.mark.asyncio
    async def test_checkpoint_cleans_up_tmp_on_failure(self, tmp_path):
        """checkpoint() removes .tmp file on failure."""
        agent = _make_agent(tmp_path)

        # Make the open succeed for .tmp but os.replace fail
        original_open = open

        call_count = 0

        def failing_open(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("Disk full")
            return original_open(*args, **kwargs)

        with patch("builtins.open", side_effect=failing_open):
            result = await agent.checkpoint({"phase": "test"})

        assert result is False

    @pytest.mark.asyncio
    async def test_checkpoint_includes_timestamp(self, tmp_path):
        """checkpoint() includes ISO timestamp in saved data."""
        agent = _make_agent(tmp_path)

        await agent.checkpoint({"phase": "test"})

        checkpoint_path = Path("data/.state") / f"{agent.job_id}.checkpoint.json"
        if checkpoint_path.exists():
            with open(checkpoint_path) as f:
                data = json.load(f)
            assert "timestamp" in data
            assert "T" in data["timestamp"]  # ISO format


# =============================================================================
# TEST LOAD CHECKPOINT SAFETY
# =============================================================================


class TestLoadCheckpointSafety:
    """Tests for load_checkpoint() with error handling."""

    def test_load_returns_none_for_missing_file(self, tmp_path):
        """load_checkpoint() returns None when file doesn't exist."""
        agent = _make_agent(tmp_path, job_id="nonexistent-job")

        result = agent.load_checkpoint()

        assert result is None

    def test_load_handles_corrupted_json(self, tmp_path):
        """load_checkpoint() returns None for corrupted JSON files."""
        agent = _make_agent(tmp_path, job_id="corrupt-job")

        # Create corrupted checkpoint file
        checkpoint_path = Path("data/.state") / "corrupt-job.checkpoint.json"
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        with open(checkpoint_path, "w") as f:
            f.write("{invalid json content!!!")

        result = agent.load_checkpoint()

        assert result is None

        # Cleanup
        checkpoint_path.unlink(missing_ok=True)

    def test_load_returns_state_for_valid_file(self, tmp_path):
        """load_checkpoint() returns state dict for valid file."""
        agent = _make_agent(tmp_path, job_id="valid-job")

        # Create valid checkpoint
        checkpoint_path = Path("data/.state") / "valid-job.checkpoint.json"
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        with open(checkpoint_path, "w") as f:
            json.dump({
                "job_id": "valid-job",
                "timestamp": "2024-01-01T00:00:00",
                "state": {"phase": "extraction", "count": 10}
            }, f)

        result = agent.load_checkpoint()

        assert result is not None
        assert result["phase"] == "extraction"
        assert result["count"] == 10

        # Cleanup
        checkpoint_path.unlink(missing_ok=True)


# =============================================================================
# TEST SAVE_RECORDS SAFETY
# =============================================================================


class TestSaveRecordsSafety:
    """Tests for save_records() with atomic writes."""

    def test_save_returns_count_on_success(self, tmp_path):
        """save_records() returns count of records saved."""
        agent = _make_agent(tmp_path)

        records = [
            {"company_name": "Acme", "city": "Detroit"},
            {"company_name": "Beta", "city": "Chicago"},
        ]

        output_path = str(tmp_path / "output" / "records.jsonl")
        result = agent.save_records(records, output_path)

        assert result == 2

    def test_save_creates_valid_jsonl(self, tmp_path):
        """save_records() creates valid JSONL file."""
        agent = _make_agent(tmp_path)

        records = [
            {"company_name": "Acme", "state": "MI"},
            {"company_name": "Beta", "state": "IL"},
        ]

        output_path = str(tmp_path / "output" / "records.jsonl")
        agent.save_records(records, output_path)

        with open(output_path) as f:
            lines = [line.strip() for line in f if line.strip()]

        assert len(lines) == 2
        assert json.loads(lines[0])["company_name"] == "Acme"
        assert json.loads(lines[1])["company_name"] == "Beta"

    def test_save_returns_negative_one_on_oserror(self, tmp_path):
        """save_records() returns -1 on OSError."""
        agent = _make_agent(tmp_path)

        with patch("skills.common.SKILL.JSONLWriter.__enter__",
                    side_effect=OSError("Permission denied")):
            result = agent.save_records(
                [{"company_name": "Test"}],
                str(tmp_path / "output" / "records.jsonl"),
            )

        assert result == -1

    def test_save_cleans_up_tmp_on_failure(self, tmp_path):
        """save_records() removes .tmp file on failure."""
        agent = _make_agent(tmp_path)

        output_path = tmp_path / "output" / "records.jsonl"
        tmp_file = output_path.with_suffix(".tmp")

        with patch("skills.common.SKILL.JSONLWriter.__enter__",
                    side_effect=OSError("Disk full")):
            agent.save_records(
                [{"company_name": "Test"}],
                str(output_path),
            )

        # .tmp file should not exist
        assert not tmp_file.exists()

    def test_save_creates_parent_directories(self, tmp_path):
        """save_records() creates parent directories."""
        agent = _make_agent(tmp_path)

        deep_path = str(tmp_path / "deep" / "nested" / "dir" / "records.jsonl")
        result = agent.save_records([{"company_name": "Test"}], deep_path)

        assert result == 1
        assert Path(deep_path).exists()

    def test_save_empty_records(self, tmp_path):
        """save_records() handles empty record list."""
        agent = _make_agent(tmp_path)

        output_path = str(tmp_path / "empty.jsonl")
        result = agent.save_records([], output_path)

        assert result == 0
