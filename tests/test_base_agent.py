"""
Tests for agents/base.py - BaseAgent class

Tests lifecycle management, hooks, utilities, and error handling.
"""

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# =============================================================================
# TEST AGENT SUBCLASS
# =============================================================================


class ConcreteTestAgent:
    """
    Concrete implementation of BaseAgent for testing.
    Must be created inside tests to avoid import issues with mocking.
    """
    pass


def create_concrete_agent(
    run_result: dict = None,
    run_error: Exception = None,
    setup_hook: callable = None
):
    """Factory to create ConcreteTestAgent with configurable behavior."""
    from agents.base import BaseAgent

    class ConcreteTestAgent(BaseAgent):
        """Test agent with configurable run() behavior."""

        def __init__(self, *args, **kwargs):
            self._run_result = run_result or {
                "success": True,
                "records_processed": 10,
                "data": []
            }
            self._run_error = run_error
            self._setup_hook = setup_hook
            super().__init__(*args, **kwargs)

        def _setup(self, **kwargs):
            if self._setup_hook:
                self._setup_hook(self, **kwargs)

        async def run(self, task: dict) -> dict:
            if self._run_error:
                raise self._run_error
            return self._run_result

    return ConcreteTestAgent


# =============================================================================
# TEST INITIALIZATION
# =============================================================================


class TestBaseAgentInitialization:
    """Tests for BaseAgent.__init__()."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_creates_with_agent_type(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent is created with specified agent_type."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()

        agent = AgentClass(agent_type="test.agent")

        assert agent.agent_type == "test.agent"

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_generates_job_id_when_not_provided(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent generates UUID job_id when not provided."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()

        agent = AgentClass(agent_type="test.agent")

        assert agent.job_id is not None
        assert len(agent.job_id) == 36  # UUID format

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_uses_provided_job_id(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent uses provided job_id."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()

        agent = AgentClass(agent_type="test.agent", job_id="custom-job-123")

        assert agent.job_id == "custom-job-123"

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_initializes_components(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent initializes all required components."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()

        AgentClass(agent_type="test.agent")

        mock_config.assert_called_once()
        mock_logger.assert_called_once()
        mock_limiter.assert_called_once()
        mock_http.assert_called_once()

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_sets_initial_state(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent starts with correct initial state."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()

        agent = AgentClass(agent_type="test.agent")

        assert agent.started_at is None
        assert agent.completed_at is None
        assert agent.status == "initialized"
        assert agent.results == {}
        assert agent.errors == []

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_stores_kwargs(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent stores additional kwargs."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()

        agent = AgentClass(
            agent_type="test.agent",
            custom_param="value"
        )

        assert agent._kwargs.get("custom_param") == "value"

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_calls_setup_hook(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent calls _setup() with kwargs."""
        mock_config.return_value.load.return_value = {}
        setup_called = []

        def setup_hook(agent, **kwargs):
            setup_called.append(kwargs)

        AgentClass = create_concrete_agent(setup_hook=setup_hook)
        AgentClass(agent_type="test.agent", extra="data")

        assert len(setup_called) == 1
        assert setup_called[0].get("extra") == "data"

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_sets_default_config_path(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent uses default config path."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()

        agent = AgentClass(agent_type="test.agent")

        assert agent.config_path == Path("config")

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_uses_custom_config_path(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent uses custom config path."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()

        agent = AgentClass(agent_type="test.agent", config_path="custom/config")

        assert agent.config_path == Path("custom/config")


class TestLoadAgentConfig:
    """Tests for BaseAgent._load_agent_config()."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_loads_agent_specific_config(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Loads config for agent type."""
        mock_config.return_value.load.return_value = {
            "extraction": {
                "html_parser": {
                    "batch_size": 100,
                    "timeout": 30
                }
            }
        }
        AgentClass = create_concrete_agent()

        agent = AgentClass(agent_type="extraction.html_parser")

        assert agent.agent_config == {"batch_size": 100, "timeout": 30}

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_returns_empty_dict_when_no_config(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Returns empty dict when config not found."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()

        agent = AgentClass(agent_type="nonexistent.agent")

        assert agent.agent_config == {}

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_handles_config_exception(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Handles exception when loading config."""
        mock_config.return_value.load.side_effect = Exception("Config error")
        AgentClass = create_concrete_agent()

        agent = AgentClass(agent_type="test.agent")

        assert agent.agent_config == {}


# =============================================================================
# TEST LIFECYCLE - SUCCESS
# =============================================================================


class TestBaseAgentLifecycleSuccess:
    """Tests for successful execute() lifecycle."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_sets_started_at(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() sets started_at timestamp."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        await agent.execute({})

        assert agent.started_at is not None
        assert isinstance(agent.started_at, datetime)

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_sets_running_status(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() sets status to running then completed."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        statuses = []

        def setup_hook(agent, **kwargs):
            pass

        AgentClass = create_concrete_agent(setup_hook=setup_hook)
        agent = AgentClass(agent_type="test.agent")

        # Capture status during run
        original_run_method = agent.run

        async def capture_run(task):
            statuses.append(agent.status)
            return await original_run_method(task)

        agent.run = capture_run

        await agent.execute({})

        assert "running" in statuses
        assert agent.status == "completed"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_returns_result(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() returns result from run()."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        expected = {"success": True, "records_processed": 5, "data": ["a", "b"]}
        AgentClass = create_concrete_agent(run_result=expected)
        agent = AgentClass(agent_type="test.agent")

        result = await agent.execute({})

        assert result["success"] is True
        assert result["records_processed"] == 5

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_adds_meta_to_result(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() adds _meta to result."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent", job_id="job-123")

        result = await agent.execute({})

        assert "_meta" in result
        assert result["_meta"]["job_id"] == "job-123"
        assert result["_meta"]["agent_type"] == "test.agent"
        assert "completed_at" in result["_meta"]
        assert "duration_seconds" in result["_meta"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_stores_results(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() stores result in self.results."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        expected = {"success": True, "records_processed": 5, "data": []}
        AgentClass = create_concrete_agent(run_result=expected)
        agent = AgentClass(agent_type="test.agent")

        await agent.execute({})

        assert agent.results["success"] is True
        assert agent.results["records_processed"] == 5

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_sets_completed_at(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() sets completed_at timestamp."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        await agent.execute({})

        assert agent.completed_at is not None
        assert agent.completed_at >= agent.started_at

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_calls_pre_execute_hook(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() calls _pre_execute hook."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent", job_id="job-123")

        original_task = {"url": "https://example.com"}
        await agent.execute(original_task)

        # _pre_execute adds _meta to task
        assert "_meta" in original_task
        assert original_task["_meta"]["job_id"] == "job-123"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_calls_cleanup(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() calls _cleanup in finally block."""
        mock_config.return_value.load.return_value = {}
        mock_close = AsyncMock()
        mock_http.return_value.close = mock_close
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        await agent.execute({})

        mock_close.assert_called_once()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_logs_start_and_completion(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() logs agent start and completion."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        mock_log_instance = MagicMock()
        mock_logger.return_value = mock_log_instance
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        await agent.execute({"url": "https://example.com"})

        # Check info was called (start and completion)
        assert mock_log_instance.info.call_count >= 2


# =============================================================================
# TEST LIFECYCLE - ERROR
# =============================================================================


class TestBaseAgentLifecycleError:
    """Tests for error handling in execute()."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_catches_exception(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() catches exceptions from run()."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        AgentClass = create_concrete_agent(run_error=ValueError("Test error"))
        agent = AgentClass(agent_type="test.agent")

        result = await agent.execute({})

        assert result["success"] is False
        assert "Test error" in result["error"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_sets_failed_status(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() sets status to failed on error."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        AgentClass = create_concrete_agent(run_error=RuntimeError("Fail"))
        agent = AgentClass(agent_type="test.agent")

        await agent.execute({})

        assert agent.status == "failed"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_records_error(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() records error in self.errors."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        AgentClass = create_concrete_agent(run_error=ValueError("Recorded error"))
        agent = AgentClass(agent_type="test.agent")

        await agent.execute({})

        assert len(agent.errors) == 1
        assert "Recorded error" in agent.errors[0]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_returns_error_type(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() returns error_type in result."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        AgentClass = create_concrete_agent(run_error=TypeError("Type error"))
        agent = AgentClass(agent_type="test.agent")

        result = await agent.execute({})

        assert result["error_type"] == "TypeError"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_calls_on_error_hook(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() calls _on_error hook."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        AgentClass = create_concrete_agent(run_error=ValueError("Hook test"))
        agent = AgentClass(agent_type="test.agent")

        result = await agent.execute({})

        assert "error_handling" in result
        assert result["error_handling"]["action"] == "logged"
        assert "traceback" in result["error_handling"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_logs_error(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() logs error on failure."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        mock_log_instance = MagicMock()
        mock_logger.return_value = mock_log_instance
        AgentClass = create_concrete_agent(run_error=ValueError("Log test"))
        agent = AgentClass(agent_type="test.agent")

        await agent.execute({})

        mock_log_instance.error.assert_called()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_cleanup_called_on_error(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() calls cleanup even on error."""
        mock_config.return_value.load.return_value = {}
        mock_close = AsyncMock()
        mock_http.return_value.close = mock_close
        AgentClass = create_concrete_agent(run_error=RuntimeError("Cleanup test"))
        agent = AgentClass(agent_type="test.agent")

        await agent.execute({})

        mock_close.assert_called_once()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_sets_records_processed_zero_on_error(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """execute() returns records_processed=0 on error."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        AgentClass = create_concrete_agent(run_error=ValueError("Zero records"))
        agent = AgentClass(agent_type="test.agent")

        result = await agent.execute({})

        assert result["records_processed"] == 0


# =============================================================================
# TEST RECORD I/O
# =============================================================================


class TestBaseAgentRecordIO:
    """Tests for save_records() and load_records()."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_save_records_creates_file(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """save_records() creates JSONL file."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        output_path = tmp_path / "output.jsonl"
        records = [{"name": "Company A"}, {"name": "Company B"}]

        agent.save_records(records, str(output_path))

        assert output_path.exists()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_save_records_writes_jsonl_format(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """save_records() writes one JSON object per line."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        output_path = tmp_path / "output.jsonl"
        records = [{"name": "A"}, {"name": "B"}, {"name": "C"}]

        agent.save_records(records, str(output_path))

        lines = output_path.read_text().strip().split("\n")
        assert len(lines) == 3
        assert json.loads(lines[0]) == {"name": "A"}
        assert json.loads(lines[1]) == {"name": "B"}
        assert json.loads(lines[2]) == {"name": "C"}

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_save_records_creates_parent_directories(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """save_records() creates parent directories if needed."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        output_path = tmp_path / "nested" / "dirs" / "output.jsonl"
        records = [{"name": "Test"}]

        agent.save_records(records, str(output_path))

        assert output_path.exists()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_load_records_reads_jsonl(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """load_records() reads JSONL file."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        # Create input file
        input_path = tmp_path / "input.jsonl"
        input_path.write_text('{"name": "X"}\n{"name": "Y"}\n')

        records = agent.load_records(str(input_path))

        assert len(records) == 2
        assert records[0] == {"name": "X"}
        assert records[1] == {"name": "Y"}

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_load_records_handles_empty_file(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """load_records() returns empty list for empty file."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        input_path = tmp_path / "empty.jsonl"
        input_path.write_text("")

        records = agent.load_records(str(input_path))

        assert records == []

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_load_records_returns_empty_for_missing_file(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """load_records() returns empty list for missing file."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        records = agent.load_records(str(tmp_path / "nonexistent.jsonl"))

        assert records == []


# =============================================================================
# TEST CHECKPOINT
# =============================================================================


class TestBaseAgentCheckpoint:
    """Tests for checkpoint() and load_checkpoint()."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_checkpoint_creates_file(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path, monkeypatch
    ):
        """checkpoint() creates checkpoint file."""
        mock_config.return_value.load.return_value = {}
        # Change to tmp_path to use relative data/.state path
        monkeypatch.chdir(tmp_path)
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent", job_id="ckpt-test-123")

        await agent.checkpoint({"page": 5, "processed": 100})

        checkpoint_path = tmp_path / "data" / ".state" / "ckpt-test-123.checkpoint.json"
        assert checkpoint_path.exists()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_checkpoint_saves_state(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path, monkeypatch
    ):
        """checkpoint() saves state in correct format."""
        mock_config.return_value.load.return_value = {}
        monkeypatch.chdir(tmp_path)
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent", job_id="ckpt-format-123")

        await agent.checkpoint({"page": 5, "items": ["a", "b"]})

        checkpoint_path = tmp_path / "data" / ".state" / "ckpt-format-123.checkpoint.json"
        data = json.loads(checkpoint_path.read_text())

        assert data["job_id"] == "ckpt-format-123"
        assert data["agent_type"] == "test.agent"
        assert "timestamp" in data
        assert data["state"] == {"page": 5, "items": ["a", "b"]}

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_load_checkpoint_returns_state(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path, monkeypatch
    ):
        """load_checkpoint() returns saved state."""
        mock_config.return_value.load.return_value = {}
        monkeypatch.chdir(tmp_path)

        # Create checkpoint file
        checkpoint_dir = tmp_path / "data" / ".state"
        checkpoint_dir.mkdir(parents=True)
        checkpoint_path = checkpoint_dir / "load-test-123.checkpoint.json"
        checkpoint_path.write_text(json.dumps({
            "job_id": "load-test-123",
            "agent_type": "test.agent",
            "timestamp": "2024-01-15T10:00:00",
            "state": {"cursor": 50}
        }))

        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent", job_id="load-test-123")

        state = agent.load_checkpoint()

        assert state == {"cursor": 50}

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_load_checkpoint_returns_none_when_missing(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path, monkeypatch
    ):
        """load_checkpoint() returns None when no checkpoint exists."""
        mock_config.return_value.load.return_value = {}
        monkeypatch.chdir(tmp_path)
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent", job_id="missing-123")

        state = agent.load_checkpoint()

        assert state is None


# =============================================================================
# TEST DURATION
# =============================================================================


class TestBaseAgentDuration:
    """Tests for _get_duration()."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_duration_zero_when_not_started(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """_get_duration() returns 0 when not started."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        assert agent._get_duration() == 0

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_duration_positive_when_started(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """_get_duration() returns positive value after start."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")
        agent.started_at = datetime.now(UTC)

        duration = agent._get_duration()

        assert duration >= 0

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_duration_uses_completed_at_when_set(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """_get_duration() uses completed_at when available."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        # Set specific times
        from datetime import timedelta
        agent.started_at = datetime.now(UTC)
        agent.completed_at = agent.started_at + timedelta(seconds=5)

        duration = agent._get_duration()

        assert abs(duration - 5.0) < 0.1


# =============================================================================
# TEST LOAD SCHEMA
# =============================================================================


class TestBaseAgentLoadSchema:
    """Tests for load_schema() method."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_load_schema_delegates_to_config(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """load_schema() loads via config."""
        mock_config.return_value.load.return_value = {
            "pma": {"company_name": {"selectors": [".name"]}}
        }
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        agent.load_schema("pma")

        mock_config.return_value.load.assert_called_with("schemas/pma")



# =============================================================================
# TEST API KEY HEALTH CHECK
# =============================================================================


class TestBaseAgentApiKeyHealthCheck:
    """Tests for _check_api_keys() method."""

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_no_requirements_returns_empty(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Agent with no API key requirements returns empty list."""
        mock_config.return_value.load.return_value = {}
        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="test.agent")

        missing = agent._check_api_keys()

        assert missing == []

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_enrichment_tech_stack_missing_key(
        self, mock_limiter, mock_http, mock_logger, mock_config, monkeypatch
    ):
        """Tech stack agent reports missing BUILTWITH_API_KEY."""
        mock_config.return_value.load.return_value = {}
        monkeypatch.delenv("BUILTWITH_API_KEY", raising=False)

        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="enrichment.tech_stack")

        missing = agent._check_api_keys()

        assert "BUILTWITH_API_KEY" in missing
        agent.log.warning.assert_called_once()
        call_args = agent.log.warning.call_args
        assert call_args[0][0] == "missing_api_keys"

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_enrichment_tech_stack_key_present(
        self, mock_limiter, mock_http, mock_logger, mock_config, monkeypatch
    ):
        """Tech stack agent verifies key when present."""
        mock_config.return_value.load.return_value = {}
        monkeypatch.setenv("BUILTWITH_API_KEY", "test-key-123")

        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="enrichment.tech_stack")

        missing = agent._check_api_keys()

        assert missing == []
        agent.log.info.assert_called()
        # Find the api_keys_verified call
        info_calls = [c for c in agent.log.info.call_args_list if c[0][0] == "api_keys_verified"]
        assert len(info_calls) == 1

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_firmographic_reports_multiple_missing(
        self, mock_limiter, mock_http, mock_logger, mock_config, monkeypatch
    ):
        """Firmographic agent reports both missing keys."""
        mock_config.return_value.load.return_value = {}
        monkeypatch.delenv("CLEARBIT_API_KEY", raising=False)
        monkeypatch.delenv("APOLLO_API_KEY", raising=False)

        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="enrichment.firmographic")

        missing = agent._check_api_keys()

        assert "CLEARBIT_API_KEY" in missing
        assert "APOLLO_API_KEY" in missing
        assert len(missing) == 2

    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    def test_firmographic_partial_keys(
        self, mock_limiter, mock_http, mock_logger, mock_config, monkeypatch
    ):
        """Firmographic agent with one key present, one missing."""
        mock_config.return_value.load.return_value = {}
        monkeypatch.setenv("CLEARBIT_API_KEY", "test-clearbit")
        monkeypatch.delenv("APOLLO_API_KEY", raising=False)

        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="enrichment.firmographic")

        missing = agent._check_api_keys()

        assert missing == ["APOLLO_API_KEY"]
        agent.log.warning.assert_called_once()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_execute_calls_check_api_keys(
        self, mock_limiter, mock_http, mock_logger, mock_config, monkeypatch
    ):
        """execute() calls _check_api_keys() before run()."""
        mock_config.return_value.load.return_value = {}
        mock_http.return_value.close = AsyncMock()
        monkeypatch.delenv("BUILTWITH_API_KEY", raising=False)

        AgentClass = create_concrete_agent()
        agent = AgentClass(agent_type="enrichment.tech_stack")

        await agent.execute({})

        # Verify warning was logged for missing key
        warning_calls = [c for c in agent.log.warning.call_args_list if c[0][0] == "missing_api_keys"]
        assert len(warning_calls) == 1
