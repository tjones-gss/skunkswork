"""
Base Agent Class
NAM Competitive Intelligence Pipeline

All agents inherit from this base class.
"""

import asyncio
import json
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, UTC
from pathlib import Path
from typing import Any, Optional

import yaml

from skills.common.SKILL import (
    AsyncHTTPClient,
    RateLimiter,
    StructuredLogger,
    JSONLWriter,
    Config,
)


class BaseAgent(ABC):
    """
    Abstract base class for all pipeline agents.
    
    Provides common functionality:
    - Configuration loading
    - Logging
    - HTTP client with rate limiting
    - State management
    - Error handling
    """
    
    def __init__(
        self,
        agent_type: str,
        job_id: str = None,
        config_path: str = "config",
        **kwargs
    ):
        self.agent_type = agent_type
        self.job_id = job_id or str(uuid.uuid4())
        self.config_path = Path(config_path)
        
        # Initialize components
        self.config = Config(config_path)
        self.log = StructuredLogger(agent_type, self.job_id)
        self.rate_limiter = RateLimiter()
        self.http = AsyncHTTPClient(self.rate_limiter)
        
        # Load agent-specific config
        self.agent_config = self._load_agent_config()
        
        # State
        self.started_at = None
        self.completed_at = None
        self.status = "initialized"
        self.results = {}
        self.errors = []
        
        # Allow subclass customization
        self._kwargs = kwargs
        self._setup(**kwargs)
    
    def _load_agent_config(self) -> dict:
        """Load agent-specific configuration."""
        try:
            agents_config = self.config.load("agents")
            
            # Parse agent type (e.g., "extraction.html_parser" -> ["extraction", "html_parser"])
            parts = self.agent_type.split(".")
            
            config = agents_config
            for part in parts:
                config = config.get(part, {})
            
            return config
        except Exception as e:
            self.log.warning(f"Could not load agent config: {e}")
            return {}
    
    def _setup(self, **kwargs):
        """
        Hook for subclass initialization.
        Override in subclasses for custom setup.
        """
        pass
    
    @abstractmethod
    async def run(self, task: dict) -> dict:
        """
        Execute the agent's main task.
        
        Args:
            task: Task configuration dictionary
            
        Returns:
            Result dictionary with at minimum:
            - success: bool
            - records_processed: int
            - data: any output data
        """
        pass
    
    async def execute(self, task: dict) -> dict:
        """
        Execute the agent with full lifecycle management.
        Wraps run() with logging, timing, and error handling.
        """
        self.started_at = datetime.now(UTC)
        self.status = "running"
        
        self.log.info("Agent starting", task_keys=list(task.keys()))
        
        try:
            # Run pre-execution hook
            task = await self._pre_execute(task)
            
            # Execute main task
            result = await self.run(task)
            
            # Run post-execution hook
            result = await self._post_execute(task, result)
            
            self.status = "completed"
            self.results = result
            
            self.log.info(
                "Agent completed",
                success=result.get("success", True),
                records_processed=result.get("records_processed", 0),
                duration_seconds=self._get_duration()
            )
            
            return result
            
        except Exception as e:
            self.status = "failed"
            self.errors.append(str(e))
            
            self.log.error(
                "Agent failed",
                error=str(e),
                error_type=type(e).__name__,
                duration_seconds=self._get_duration()
            )
            
            # Run error hook
            error_result = await self._on_error(task, e)
            
            return {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "error_handling": error_result,
                "records_processed": 0
            }
        
        finally:
            self.completed_at = datetime.now(UTC)
            await self._cleanup()
    
    async def _pre_execute(self, task: dict) -> dict:
        """Pre-execution hook. Can modify task."""
        # Add metadata
        task["_meta"] = {
            "job_id": self.job_id,
            "agent_type": self.agent_type,
            "started_at": self.started_at.isoformat()
        }
        return task
    
    async def _post_execute(self, task: dict, result: dict) -> dict:
        """Post-execution hook. Can modify result."""
        result["_meta"] = {
            "job_id": self.job_id,
            "agent_type": self.agent_type,
            "completed_at": datetime.now(UTC).isoformat(),
            "duration_seconds": self._get_duration()
        }
        return result
    
    async def _on_error(self, task: dict, error: Exception) -> dict:
        """Error handling hook."""
        import traceback
        
        return {
            "action": "logged",
            "traceback": traceback.format_exc()
        }
    
    async def _cleanup(self):
        """Cleanup hook. Called after execution completes."""
        await self.http.close()
    
    def _get_duration(self) -> float:
        """Get execution duration in seconds."""
        if not self.started_at:
            return 0
        
        end = self.completed_at or datetime.now(UTC)
        return (end - self.started_at).total_seconds()
    
    # Utility methods for subclasses
    
    def load_schema(self, schema_name: str) -> dict:
        """Load an extraction schema."""
        schemas = self.config.load(f"schemas/{schema_name}")
        return schemas.get(schema_name, schemas.get("default", {}))
    
    def save_records(self, records: list[dict], output_path: str):
        """Save records to JSONL file."""
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        
        with JSONLWriter(str(path)) as writer:
            writer.write_batch(records)
        
        self.log.info(f"Saved {len(records)} records to {output_path}")
    
    def load_records(self, input_path: str) -> list[dict]:
        """Load records from JSONL file."""
        from skills.common.SKILL import JSONLReader
        
        reader = JSONLReader(input_path)
        return reader.read_all()
    
    async def checkpoint(self, state: dict):
        """Save a checkpoint for recovery."""
        checkpoint_path = Path("data/.state") / f"{self.job_id}.checkpoint.json"
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        
        checkpoint = {
            "job_id": self.job_id,
            "agent_type": self.agent_type,
            "timestamp": datetime.now(UTC).isoformat(),
            "state": state
        }
        
        with open(checkpoint_path, "w") as f:
            json.dump(checkpoint, f)
        
        self.log.debug("Checkpoint saved", state_keys=list(state.keys()))
    
    def load_checkpoint(self) -> Optional[dict]:
        """Load a checkpoint if it exists."""
        checkpoint_path = Path("data/.state") / f"{self.job_id}.checkpoint.json"
        
        if not checkpoint_path.exists():
            return None
        
        with open(checkpoint_path) as f:
            checkpoint = json.load(f)
        
        self.log.info("Checkpoint loaded", timestamp=checkpoint.get("timestamp"))
        return checkpoint.get("state")


class AgentSpawner:
    """
    Spawns and manages sub-agents.
    Used by the orchestrator to run parallel tasks.
    """
    
    # Registry of agent classes
    AGENT_REGISTRY = {
        # Discovery agents
        "discovery.site_mapper": "agents.discovery.site_mapper.SiteMapperAgent",
        "discovery.link_crawler": "agents.discovery.link_crawler.LinkCrawlerAgent",
        "discovery.access_gatekeeper": "agents.discovery.access_gatekeeper.AccessGatekeeperAgent",
        "discovery.page_classifier": "agents.discovery.page_classifier.PageClassifierAgent",

        # Extraction agents
        "extraction.html_parser": "agents.extraction.html_parser.HTMLParserAgent",
        "extraction.directory_parser": "agents.extraction.html_parser.DirectoryParserAgent",
        "extraction.api_client": "agents.extraction.api_client.APIClientAgent",
        "extraction.pdf_parser": "agents.extraction.pdf_parser.PDFParserAgent",
        "extraction.event_extractor": "agents.extraction.event_extractor.EventExtractorAgent",
        "extraction.event_participant_extractor": "agents.extraction.event_participant_extractor.EventParticipantExtractorAgent",

        # Enrichment agents
        "enrichment.firmographic": "agents.enrichment.firmographic.FirmographicAgent",
        "enrichment.tech_stack": "agents.enrichment.tech_stack.TechStackAgent",
        "enrichment.contact_finder": "agents.enrichment.contact_finder.ContactFinderAgent",

        # Validation agents
        "validation.dedupe": "agents.validation.dedupe.DedupeAgent",
        "validation.crossref": "agents.validation.crossref.CrossRefAgent",
        "validation.scorer": "agents.validation.scorer.ScorerAgent",
        "validation.entity_resolver": "agents.validation.entity_resolver.EntityResolverAgent",

        # Intelligence agents
        "intelligence.competitor_signal_miner": "agents.intelligence.competitor_signal_miner.CompetitorSignalMinerAgent",
        "intelligence.relationship_graph_builder": "agents.intelligence.relationship_graph_builder.RelationshipGraphBuilderAgent",

        # Export agents
        "export.export_activation": "agents.export.export_activation.ExportActivationAgent",

        # Monitoring agents
        "monitoring.source_monitor": "agents.monitoring.source_monitor.SourceMonitorAgent",
    }
    
    def __init__(self, job_id: str = None):
        self.job_id = job_id or str(uuid.uuid4())
        self.log = StructuredLogger("spawner", self.job_id)
    
    def _load_agent_class(self, agent_type: str):
        """Dynamically load an agent class."""
        if agent_type not in self.AGENT_REGISTRY:
            raise ValueError(f"Unknown agent type: {agent_type}")
        
        module_path = self.AGENT_REGISTRY[agent_type]
        module_name, class_name = module_path.rsplit(".", 1)
        
        import importlib
        module = importlib.import_module(module_name)
        return getattr(module, class_name)
    
    async def spawn(
        self,
        agent_type: str,
        task: dict,
        timeout: int = 300
    ) -> dict:
        """
        Spawn a single agent and wait for completion.
        
        Args:
            agent_type: Type of agent to spawn
            task: Task configuration
            timeout: Maximum execution time in seconds
            
        Returns:
            Agent result dictionary
        """
        self.log.info(f"Spawning agent: {agent_type}")
        
        try:
            AgentClass = self._load_agent_class(agent_type)
            agent = AgentClass(agent_type=agent_type, job_id=self.job_id)
            
            # Execute with timeout
            result = await asyncio.wait_for(
                agent.execute(task),
                timeout=timeout
            )
            
            return result
            
        except asyncio.TimeoutError:
            self.log.error(f"Agent {agent_type} timed out after {timeout}s")
            return {
                "success": False,
                "error": f"Timeout after {timeout}s",
                "error_type": "TimeoutError",
                "records_processed": 0
            }
        except Exception as e:
            self.log.error(f"Failed to spawn agent {agent_type}: {e}")
            return {
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "records_processed": 0
            }
    
    async def spawn_parallel(
        self,
        agent_type: str,
        tasks: list[dict],
        max_concurrent: int = 5,
        timeout: int = 300
    ) -> list[dict]:
        """
        Spawn multiple agents in parallel.
        
        Args:
            agent_type: Type of agent to spawn
            tasks: List of task configurations
            max_concurrent: Maximum concurrent agents
            timeout: Timeout per agent
            
        Returns:
            List of results (in same order as tasks)
        """
        self.log.info(
            f"Spawning {len(tasks)} agents of type {agent_type}",
            max_concurrent=max_concurrent
        )
        
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def limited_spawn(task):
            async with semaphore:
                return await self.spawn(agent_type, task, timeout)
        
        results = await asyncio.gather(
            *[limited_spawn(task) for task in tasks],
            return_exceptions=True
        )
        
        # Convert exceptions to error results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "success": False,
                    "error": str(result),
                    "error_type": type(result).__name__,
                    "records_processed": 0
                })
            else:
                processed_results.append(result)
        
        # Log summary
        successes = sum(1 for r in processed_results if r.get("success", False))
        total_records = sum(r.get("records_processed", 0) for r in processed_results)
        
        self.log.info(
            "Parallel spawn complete",
            total=len(tasks),
            successes=successes,
            failures=len(tasks) - successes,
            total_records=total_records
        )
        
        return processed_results
