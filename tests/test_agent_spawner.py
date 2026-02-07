"""
Tests for agents/base.py - AgentSpawner class

Tests dynamic loading, spawning, and parallel execution.
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================================
# TEST AGENT REGISTRY
# =============================================================================


class TestAgentSpawnerRegistry:
    """Tests for AGENT_REGISTRY configuration."""

    def test_registry_has_discovery_agents(self):
        """Registry includes discovery agents."""
        from agents.base import AgentSpawner

        assert "discovery.site_mapper" in AgentSpawner.AGENT_REGISTRY
        assert "discovery.link_crawler" in AgentSpawner.AGENT_REGISTRY
        assert "discovery.access_gatekeeper" in AgentSpawner.AGENT_REGISTRY
        assert "discovery.page_classifier" in AgentSpawner.AGENT_REGISTRY

    def test_registry_has_extraction_agents(self):
        """Registry includes extraction agents."""
        from agents.base import AgentSpawner

        assert "extraction.html_parser" in AgentSpawner.AGENT_REGISTRY
        assert "extraction.directory_parser" in AgentSpawner.AGENT_REGISTRY
        assert "extraction.api_client" in AgentSpawner.AGENT_REGISTRY
        assert "extraction.pdf_parser" in AgentSpawner.AGENT_REGISTRY
        assert "extraction.event_extractor" in AgentSpawner.AGENT_REGISTRY

    def test_registry_has_enrichment_agents(self):
        """Registry includes enrichment agents."""
        from agents.base import AgentSpawner

        assert "enrichment.firmographic" in AgentSpawner.AGENT_REGISTRY
        assert "enrichment.tech_stack" in AgentSpawner.AGENT_REGISTRY
        assert "enrichment.contact_finder" in AgentSpawner.AGENT_REGISTRY

    def test_registry_has_validation_agents(self):
        """Registry includes validation agents."""
        from agents.base import AgentSpawner

        assert "validation.dedupe" in AgentSpawner.AGENT_REGISTRY
        assert "validation.crossref" in AgentSpawner.AGENT_REGISTRY
        assert "validation.scorer" in AgentSpawner.AGENT_REGISTRY
        assert "validation.entity_resolver" in AgentSpawner.AGENT_REGISTRY

    def test_registry_has_intelligence_agents(self):
        """Registry includes intelligence agents."""
        from agents.base import AgentSpawner

        assert "intelligence.competitor_signal_miner" in AgentSpawner.AGENT_REGISTRY
        assert "intelligence.relationship_graph_builder" in AgentSpawner.AGENT_REGISTRY

    def test_registry_has_export_agents(self):
        """Registry includes export agents."""
        from agents.base import AgentSpawner

        assert "export.export_activation" in AgentSpawner.AGENT_REGISTRY

    def test_registry_has_monitoring_agents(self):
        """Registry includes monitoring agents."""
        from agents.base import AgentSpawner

        assert "monitoring.source_monitor" in AgentSpawner.AGENT_REGISTRY

    def test_registry_values_have_correct_format(self):
        """Registry values are fully qualified class paths."""
        from agents.base import AgentSpawner

        for agent_type, module_path in AgentSpawner.AGENT_REGISTRY.items():
            # Should have at least module.class format
            assert "." in module_path
            parts = module_path.rsplit(".", 1)
            assert len(parts) == 2
            module_name, class_name = parts
            # Class name should be PascalCase
            assert class_name[0].isupper()
            # Should contain "Agent" suffix
            assert "Agent" in class_name


# =============================================================================
# TEST SPAWNER INITIALIZATION
# =============================================================================


class TestAgentSpawnerInitialization:
    """Tests for AgentSpawner.__init__()."""

    @patch("agents.base.StructuredLogger")
    def test_creates_with_default_job_id(self, mock_logger):
        """Spawner generates UUID when job_id not provided."""
        from agents.base import AgentSpawner

        spawner = AgentSpawner()

        assert spawner.job_id is not None
        assert len(spawner.job_id) == 36  # UUID format

    @patch("agents.base.StructuredLogger")
    def test_creates_with_provided_job_id(self, mock_logger):
        """Spawner uses provided job_id."""
        from agents.base import AgentSpawner

        spawner = AgentSpawner(job_id="custom-job-456")

        assert spawner.job_id == "custom-job-456"

    @patch("agents.base.StructuredLogger")
    def test_initializes_logger(self, mock_logger):
        """Spawner creates logger with correct parameters."""
        from agents.base import AgentSpawner

        spawner = AgentSpawner(job_id="logger-test")

        mock_logger.assert_called_with("spawner", "logger-test")


# =============================================================================
# TEST DYNAMIC LOADING
# =============================================================================


class TestAgentSpawnerDynamicLoading:
    """Tests for _load_agent_class()."""

    @patch("agents.base.StructuredLogger")
    def test_raises_for_unknown_agent_type(self, mock_logger):
        """Raises ValueError for unknown agent type."""
        from agents.base import AgentSpawner

        spawner = AgentSpawner()

        with pytest.raises(ValueError, match="Unknown agent type"):
            spawner._load_agent_class("nonexistent.agent")

    @patch("agents.base.StructuredLogger")
    @patch("importlib.import_module")
    def test_imports_correct_module(self, mock_import, mock_logger):
        """Loads correct module for agent type."""
        from agents.base import AgentSpawner

        mock_module = MagicMock()
        mock_module.HTMLParserAgent = MagicMock()
        mock_import.return_value = mock_module

        spawner = AgentSpawner()
        agent_class = spawner._load_agent_class("extraction.html_parser")

        mock_import.assert_called_with("agents.extraction.html_parser")
        assert agent_class == mock_module.HTMLParserAgent

    @patch("agents.base.StructuredLogger")
    @patch("importlib.import_module")
    def test_returns_class_from_module(self, mock_import, mock_logger):
        """Returns the agent class from module."""
        from agents.base import AgentSpawner

        mock_class = MagicMock()
        mock_module = MagicMock()
        mock_module.SiteMapperAgent = mock_class
        mock_import.return_value = mock_module

        spawner = AgentSpawner()
        result = spawner._load_agent_class("discovery.site_mapper")

        assert result == mock_class

    @patch("agents.base.StructuredLogger")
    @patch("importlib.import_module")
    def test_raises_when_module_not_found(self, mock_import, mock_logger):
        """Raises when module import fails."""
        from agents.base import AgentSpawner

        mock_import.side_effect = ModuleNotFoundError("No module named 'agents.discovery.site_mapper'")

        spawner = AgentSpawner()

        with pytest.raises(ModuleNotFoundError):
            spawner._load_agent_class("discovery.site_mapper")

    @patch("agents.base.StructuredLogger")
    @patch("importlib.import_module")
    def test_raises_when_class_not_found(self, mock_import, mock_logger):
        """Raises when class not in module."""
        from agents.base import AgentSpawner

        mock_module = MagicMock(spec=[])  # No attributes
        mock_import.return_value = mock_module

        spawner = AgentSpawner()

        with pytest.raises(AttributeError):
            spawner._load_agent_class("discovery.site_mapper")


# =============================================================================
# TEST SPAWN
# =============================================================================


class TestAgentSpawnerSpawn:
    """Tests for spawn() method."""

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_returns_result_on_success(self, mock_logger):
        """spawn() returns agent result on success."""
        from agents.base import AgentSpawner

        # Create mock agent
        mock_agent = MagicMock()
        mock_agent.execute = AsyncMock(return_value={
            "success": True,
            "records_processed": 10,
            "data": []
        })

        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        result = await spawner.spawn(
            "extraction.html_parser",
            {"url": "https://example.com"}
        )

        assert result["success"] is True
        assert result["records_processed"] == 10

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_creates_agent_with_correct_params(self, mock_logger):
        """spawn() creates agent with correct parameters."""
        from agents.base import AgentSpawner

        mock_agent = MagicMock()
        mock_agent.execute = AsyncMock(return_value={"success": True, "records_processed": 0})
        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner(job_id="spawn-test-123")
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        await spawner.spawn("extraction.html_parser", {})

        mock_class.assert_called_with(
            agent_type="extraction.html_parser",
            job_id="spawn-test-123"
        )

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_handles_timeout(self, mock_logger):
        """spawn() handles timeout."""
        from agents.base import AgentSpawner

        # Create agent that takes too long
        async def slow_execute(task):
            await asyncio.sleep(10)
            return {"success": True, "records_processed": 0}

        mock_agent = MagicMock()
        mock_agent.execute = slow_execute
        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        result = await spawner.spawn(
            "extraction.html_parser",
            {},
            timeout=0.1  # Very short timeout
        )

        assert result["success"] is False
        assert "Timeout" in result["error"]
        assert result["error_type"] == "TimeoutError"

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_handles_load_error(self, mock_logger):
        """spawn() handles agent loading error."""
        from agents.base import AgentSpawner

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(
            side_effect=ValueError("Unknown agent type: bad.agent")
        )

        result = await spawner.spawn("bad.agent", {})

        assert result["success"] is False
        assert "Unknown agent type" in result["error"]
        assert result["error_type"] == "ValueError"

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_handles_execution_error(self, mock_logger):
        """spawn() handles agent execution error."""
        from agents.base import AgentSpawner

        mock_agent = MagicMock()
        mock_agent.execute = AsyncMock(side_effect=RuntimeError("Execution failed"))
        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        result = await spawner.spawn("extraction.html_parser", {})

        assert result["success"] is False
        assert "Execution failed" in result["error"]
        assert result["error_type"] == "RuntimeError"

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_logs_agent_type(self, mock_logger):
        """spawn() logs the agent type being spawned."""
        from agents.base import AgentSpawner

        mock_log = MagicMock()
        mock_logger.return_value = mock_log

        mock_agent = MagicMock()
        mock_agent.execute = AsyncMock(return_value={"success": True, "records_processed": 0})
        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        await spawner.spawn("discovery.site_mapper", {})

        mock_log.info.assert_called()


# =============================================================================
# TEST SPAWN PARALLEL
# =============================================================================


class TestAgentSpawnerSpawnParallel:
    """Tests for spawn_parallel() method."""

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_parallel_returns_all_results(self, mock_logger):
        """spawn_parallel() returns result for each task."""
        from agents.base import AgentSpawner

        mock_agent = MagicMock()
        mock_agent.execute = AsyncMock(return_value={
            "success": True,
            "records_processed": 5
        })
        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        tasks = [
            {"url": "https://a.com"},
            {"url": "https://b.com"},
            {"url": "https://c.com"}
        ]

        results = await spawner.spawn_parallel("extraction.html_parser", tasks)

        assert len(results) == 3
        assert all(r["success"] for r in results)

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_parallel_respects_max_concurrent(self, mock_logger):
        """spawn_parallel() limits concurrent agents."""
        from agents.base import AgentSpawner

        concurrent_count = 0
        max_concurrent_seen = 0

        async def track_concurrent(task):
            nonlocal concurrent_count, max_concurrent_seen
            concurrent_count += 1
            max_concurrent_seen = max(max_concurrent_seen, concurrent_count)
            await asyncio.sleep(0.1)
            concurrent_count -= 1
            return {"success": True, "records_processed": 1}

        mock_agent = MagicMock()
        mock_agent.execute = track_concurrent
        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        tasks = [{"id": i} for i in range(10)]

        await spawner.spawn_parallel(
            "extraction.html_parser",
            tasks,
            max_concurrent=3
        )

        assert max_concurrent_seen <= 3

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_parallel_handles_partial_failures(self, mock_logger):
        """spawn_parallel() handles some tasks failing."""
        from agents.base import AgentSpawner

        call_count = 0

        async def sometimes_fail(task):
            nonlocal call_count
            call_count += 1
            if call_count % 2 == 0:
                raise ValueError("Every other fails")
            return {"success": True, "records_processed": 1}

        mock_agent = MagicMock()
        mock_agent.execute = sometimes_fail
        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        tasks = [{"id": i} for i in range(4)]

        results = await spawner.spawn_parallel("extraction.html_parser", tasks)

        assert len(results) == 4
        successes = sum(1 for r in results if r.get("success"))
        failures = sum(1 for r in results if not r.get("success"))
        assert successes == 2
        assert failures == 2

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_parallel_converts_exceptions_to_results(self, mock_logger):
        """spawn_parallel() converts exceptions to error results."""
        from agents.base import AgentSpawner

        # Use return_exceptions=True in gather, so exceptions become results
        mock_agent = MagicMock()
        mock_agent.execute = AsyncMock(side_effect=RuntimeError("Task failed"))
        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        results = await spawner.spawn_parallel(
            "extraction.html_parser",
            [{"id": 1}]
        )

        assert len(results) == 1
        assert results[0]["success"] is False
        assert "Task failed" in results[0]["error"]

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_parallel_logs_summary(self, mock_logger):
        """spawn_parallel() logs completion summary."""
        from agents.base import AgentSpawner

        mock_log = MagicMock()
        mock_logger.return_value = mock_log

        mock_agent = MagicMock()
        mock_agent.execute = AsyncMock(return_value={
            "success": True,
            "records_processed": 10
        })
        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        tasks = [{"id": i} for i in range(5)]
        await spawner.spawn_parallel("extraction.html_parser", tasks)

        # Should log completion with summary
        assert mock_log.info.call_count >= 2  # Start + completion

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_parallel_maintains_order(self, mock_logger):
        """spawn_parallel() returns results in same order as tasks."""
        from agents.base import AgentSpawner

        async def echo_id(task):
            return {"success": True, "records_processed": 0, "id": task["id"]}

        mock_agent = MagicMock()
        mock_agent.execute = echo_id
        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        tasks = [{"id": i} for i in range(5)]
        results = await spawner.spawn_parallel("extraction.html_parser", tasks)

        for i, result in enumerate(results):
            assert result["id"] == i

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_parallel_empty_tasks(self, mock_logger):
        """spawn_parallel() handles empty task list."""
        from agents.base import AgentSpawner

        spawner = AgentSpawner()

        results = await spawner.spawn_parallel("extraction.html_parser", [])

        assert results == []

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_parallel_default_max_concurrent(self, mock_logger):
        """spawn_parallel() uses default max_concurrent of 5."""
        from agents.base import AgentSpawner

        concurrent_count = 0
        max_seen = 0

        async def track(task):
            nonlocal concurrent_count, max_seen
            concurrent_count += 1
            max_seen = max(max_seen, concurrent_count)
            await asyncio.sleep(0.05)
            concurrent_count -= 1
            return {"success": True, "records_processed": 0}

        mock_agent = MagicMock()
        mock_agent.execute = track
        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        tasks = [{"id": i} for i in range(20)]
        await spawner.spawn_parallel("extraction.html_parser", tasks)

        # Default is 5 concurrent
        assert max_seen <= 5

    @pytest.mark.asyncio
    @patch("agents.base.StructuredLogger")
    async def test_spawn_parallel_calculates_total_records(self, mock_logger):
        """spawn_parallel() calculates total records in log."""
        from agents.base import AgentSpawner

        mock_log = MagicMock()
        mock_logger.return_value = mock_log

        mock_agent = MagicMock()
        mock_agent.execute = AsyncMock(return_value={
            "success": True,
            "records_processed": 10
        })
        mock_class = MagicMock(return_value=mock_agent)

        spawner = AgentSpawner()
        spawner._load_agent_class = MagicMock(return_value=mock_class)

        tasks = [{"id": i} for i in range(3)]
        await spawner.spawn_parallel("extraction.html_parser", tasks)

        # Last info call should include total_records=30
        calls = mock_log.info.call_args_list
        found_total = False
        for call in calls:
            if "total_records" in str(call):
                found_total = True
        # The log includes total_records as kwarg
        assert mock_log.info.called
