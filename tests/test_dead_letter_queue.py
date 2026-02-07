"""
Tests for DeadLetterQueue.

Phase 4: Dead-Letter Queue
"""

import json
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

# =============================================================================
# TEST DLQ INITIALIZATION
# =============================================================================


class TestDeadLetterQueueInit:
    """Tests for DLQ initialization."""

    def test_creates_queue_directory(self, tmp_path):
        """DLQ creates the queue directory on init."""
        from agents.base import DeadLetterQueue

        dlq_dir = tmp_path / "dlq"
        DeadLetterQueue(queue_dir=str(dlq_dir))

        assert dlq_dir.exists()

    def test_file_path_includes_date(self, tmp_path):
        """DLQ file path includes today's date."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))
        today = datetime.now(UTC).strftime("%Y%m%d")

        assert today in dlq._file_path.name

    def test_file_path_is_jsonl(self, tmp_path):
        """DLQ file has .jsonl extension."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))

        assert dlq._file_path.suffix == ".jsonl"


# =============================================================================
# TEST PUSH
# =============================================================================


class TestDeadLetterQueuePush:
    """Tests for push() method."""

    def test_push_writes_valid_jsonl(self, tmp_path):
        """push() writes valid JSONL entry."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))
        dlq.push(
            task={"url": "https://example.com"},
            error="HTTP 502",
            agent_type="extraction.html_parser",
        )

        content = dlq._file_path.read_text().strip()
        entry = json.loads(content)

        assert entry["error"] == "HTTP 502"
        assert entry["agent_type"] == "extraction.html_parser"
        assert entry["task"]["url"] == "https://example.com"
        assert "timestamp" in entry

    def test_push_appends_multiple_entries(self, tmp_path):
        """push() appends entries, doesn't overwrite."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))

        dlq.push({"id": 1}, "Error 1", "agent_a")
        dlq.push({"id": 2}, "Error 2", "agent_b")
        dlq.push({"id": 3}, "Error 3", "agent_c")

        lines = dlq._file_path.read_text().strip().split("\n")
        assert len(lines) == 3

    def test_push_with_context(self, tmp_path):
        """push() includes context dict."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))
        dlq.push(
            task={"url": "https://example.com"},
            error="Timeout",
            agent_type="extraction.html_parser",
            context={"task_index": 5, "batch": "b1"},
        )

        content = dlq._file_path.read_text().strip()
        entry = json.loads(content)

        assert entry["context"]["task_index"] == 5
        assert entry["context"]["batch"] == "b1"

    def test_push_with_exception_error(self, tmp_path):
        """push() handles exception strings."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))
        dlq.push(
            task={"url": "https://example.com"},
            error=str(ValueError("bad value")),
            agent_type="test",
        )

        content = dlq._file_path.read_text().strip()
        entry = json.loads(content)

        assert "bad value" in entry["error"]

    def test_push_with_nested_dict_task(self, tmp_path):
        """push() handles nested dict tasks."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))
        dlq.push(
            task={"url": "https://example.com", "config": {"schema": "pma", "fields": ["a", "b"]}},
            error="Parse error",
        )

        content = dlq._file_path.read_text().strip()
        entry = json.loads(content)

        assert entry["task"]["config"]["schema"] == "pma"

    def test_push_handles_write_failure_gracefully(self, tmp_path):
        """push() does not crash on write failure."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))

        with patch("builtins.open", side_effect=OSError("Disk full")):
            # Should not raise
            dlq.push({"url": "https://example.com"}, "Error")

    def test_push_default_context_is_empty_dict(self, tmp_path):
        """push() defaults context to empty dict."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))
        dlq.push({"id": 1}, "Error")

        content = dlq._file_path.read_text().strip()
        entry = json.loads(content)

        assert entry["context"] == {}

    def test_push_default_agent_type_empty(self, tmp_path):
        """push() defaults agent_type to empty string."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))
        dlq.push({"id": 1}, "Error")

        content = dlq._file_path.read_text().strip()
        entry = json.loads(content)

        assert entry["agent_type"] == ""


# =============================================================================
# TEST READ_ALL
# =============================================================================


class TestDeadLetterQueueReadAll:
    """Tests for read_all() method."""

    def test_read_all_returns_all_entries(self, tmp_path):
        """read_all() returns all pushed entries."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))

        dlq.push({"id": 1}, "Error 1", "agent_a")
        dlq.push({"id": 2}, "Error 2", "agent_b")

        entries = dlq.read_all()

        assert len(entries) == 2
        assert entries[0]["task"]["id"] == 1
        assert entries[1]["task"]["id"] == 2

    def test_read_all_returns_empty_for_no_file(self, tmp_path):
        """read_all() returns empty list when file doesn't exist."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))

        entries = dlq.read_all()

        assert entries == []

    def test_read_all_skips_invalid_json(self, tmp_path):
        """read_all() skips lines with invalid JSON."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))

        # Write valid + invalid lines
        with open(dlq._file_path, "w") as f:
            f.write(json.dumps({"task": {"id": 1}, "error": "E1"}) + "\n")
            f.write("{invalid json}\n")
            f.write(json.dumps({"task": {"id": 2}, "error": "E2"}) + "\n")

        entries = dlq.read_all()

        assert len(entries) == 2


# =============================================================================
# TEST COUNT
# =============================================================================


class TestDeadLetterQueueCount:
    """Tests for count() method."""

    def test_count_returns_correct_number(self, tmp_path):
        """count() returns correct number of entries."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))

        dlq.push({"id": 1}, "Error 1")
        dlq.push({"id": 2}, "Error 2")
        dlq.push({"id": 3}, "Error 3")

        assert dlq.count() == 3

    def test_count_returns_zero_for_empty(self, tmp_path):
        """count() returns 0 when no entries."""
        from agents.base import DeadLetterQueue

        dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))

        assert dlq.count() == 0


# =============================================================================
# TEST DLQ INTEGRATION WITH AGENTSPAWNER
# =============================================================================


class TestDLQSpawnerIntegration:
    """Tests for DLQ integration with AgentSpawner."""

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_failure_creates_dlq_entry(self, mock_logger, tmp_path):
        """spawn() failure pushes task to DLQ."""
        from agents.base import AgentSpawner, DeadLetterQueue

        spawner = AgentSpawner()
        spawner.dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))

        # Make agent loading fail
        spawner._load_agent_class = MagicMock(
            side_effect=ValueError("Unknown agent type: bad.agent")
        )

        result = await spawner.spawn("bad.agent", {"url": "https://example.com"})

        assert result["success"] is False
        assert spawner.dlq.count() == 1

        entries = spawner.dlq.read_all()
        assert entries[0]["task"]["url"] == "https://example.com"

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_timeout_creates_dlq_entry(self, mock_logger, tmp_path):
        """spawn() timeout pushes task to DLQ."""
        import asyncio

        from agents.base import AgentSpawner, DeadLetterQueue

        spawner = AgentSpawner()
        spawner.dlq = DeadLetterQueue(queue_dir=str(tmp_path / "dlq"))

        # Create slow agent
        async def slow_execute(task):
            await asyncio.sleep(10)
            return {"success": True}

        mock_agent = MagicMock()
        mock_agent.execute = slow_execute
        mock_class = MagicMock(return_value=mock_agent)
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        result = await spawner.spawn(
            "extraction.html_parser",
            {"url": "https://example.com"},
            timeout=0.1,
        )

        assert result["success"] is False
        assert spawner.dlq.count() == 1
