"""
Orchestrator Agent
NAM Competitive Intelligence Pipeline

Central coordinator for all pipeline operations using state machine.
"""

import asyncio
import json
import uuid
from datetime import datetime, UTC
from pathlib import Path
from typing import Optional

import click

from agents.base import BaseAgent, AgentSpawner
from state.machine import PipelineState, PipelinePhase, StateManager


class OrchestratorAgent(BaseAgent):
    """
    Orchestrator Agent - coordinates all pipeline operations using state machine.

    Pipeline Phases:
    INIT → GATEKEEPER → DISCOVERY → CLASSIFICATION → EXTRACTION →
    ENRICHMENT → VALIDATION → RESOLUTION → GRAPH → EXPORT → MONITOR → DONE

    Modes:
    - extract: Extract data from association directories
    - enrich: Enrich records with firmographic/tech/contact data
    - validate: Deduplicate, validate, and score records
    - full: Run complete state-machine pipeline
    """

    def _setup(self, **kwargs):
        """Initialize orchestrator-specific components."""
        self.spawner = AgentSpawner(self.job_id)
        self.associations_config = self.config.load("associations")
        self.mode = kwargs.get("mode", "full")
        self.associations = kwargs.get("associations", [])
        self.dry_run = kwargs.get("dry_run", False)

        # Error threshold: maximum acceptable error rate (0.0-1.0)
        self.max_error_rate = float(
            self.agent_config.get("max_extraction_errors", 0.5)
        )

        # State management
        self.state_manager = StateManager()
        self.state: Optional[PipelineState] = None

    async def run(self, task: dict) -> dict:
        """
        Execute orchestrator based on mode.
        """
        mode = task.get("mode", self.mode)
        associations = task.get("associations", self.associations)
        resume_job_id = task.get("resume_job_id")

        self.log.info(f"Orchestrator running in {mode} mode", associations=associations)

        # Handle resume
        if resume_job_id:
            self.state = self.state_manager.load_state(resume_job_id)
            if self.state:
                self.log.info(f"Resuming job {resume_job_id} from phase {self.state.current_phase}")
                return await self._run_state_machine()

        if mode == "extract":
            return await self._run_extraction(associations)
        elif mode == "extract-all":
            all_associations = list(self.associations_config.get("associations", {}).keys())
            return await self._run_extraction(all_associations)
        elif mode == "enrich":
            return await self._run_enrichment(task.get("enrichment", "all"))
        elif mode == "enrich-all":
            return await self._run_enrichment("all")
        elif mode == "validate":
            return await self._run_validation(task.get("validation", "all"))
        elif mode == "validate-all":
            return await self._run_validation("all")
        elif mode == "full":
            return await self._run_full_pipeline_state_machine(associations)
        else:
            raise ValueError(f"Unknown mode: {mode}")

    # =========================================================================
    # STATE MACHINE PIPELINE
    # =========================================================================

    async def _run_full_pipeline_state_machine(self, associations: list[str]) -> dict:
        """
        Run complete pipeline using state machine with checkpoint/resume.
        """
        # Create new pipeline state
        self.state = self.state_manager.create_state(
            associations=associations,
            job_id=self.job_id
        )

        self.log.info(
            f"Starting state-machine pipeline",
            job_id=self.state.job_id,
            associations=associations
        )

        return await self._run_state_machine()

    async def _run_state_machine(self) -> dict:
        """Execute state machine from current phase."""
        try:
            while self.state.current_phase not in [PipelinePhase.DONE, PipelinePhase.FAILED]:
                phase = self.state.current_phase

                self.log.info(f"Executing phase: {phase.value}")

                # Execute phase
                success = await self._execute_phase(phase)

                if not success:
                    self.state_manager.transition_phase(self.state, PipelinePhase.FAILED)
                    break

                # Transition to next phase
                next_phase = self._get_next_phase(phase)
                if next_phase:
                    self.state_manager.transition_phase(self.state, next_phase)
                else:
                    break

            # Generate final result
            return self._build_final_result()

        except Exception as e:
            self.log.error(f"Pipeline failed: {e}")
            self.state.add_error({
                "phase": self.state.current_phase.value,
                "agent": "orchestrator",
                "error_type": type(e).__name__,
                "error_message": str(e)
            })
            self.state_manager.transition_phase(self.state, PipelinePhase.FAILED)
            return self._build_final_result()

    async def _execute_phase(self, phase: PipelinePhase) -> bool:
        """Execute a single pipeline phase."""
        phase_handlers = {
            PipelinePhase.INIT: self._phase_init,
            PipelinePhase.GATEKEEPER: self._phase_gatekeeper,
            PipelinePhase.DISCOVERY: self._phase_discovery,
            PipelinePhase.CLASSIFICATION: self._phase_classification,
            PipelinePhase.EXTRACTION: self._phase_extraction,
            PipelinePhase.ENRICHMENT: self._phase_enrichment,
            PipelinePhase.VALIDATION: self._phase_validation,
            PipelinePhase.RESOLUTION: self._phase_resolution,
            PipelinePhase.GRAPH: self._phase_graph,
            PipelinePhase.EXPORT: self._phase_export,
            PipelinePhase.MONITOR: self._phase_monitor,
        }

        handler = phase_handlers.get(phase)
        if handler:
            return await handler()

        return True

    def _get_next_phase(self, current: PipelinePhase) -> Optional[PipelinePhase]:
        """Get next phase in pipeline."""
        phase_order = [
            PipelinePhase.INIT,
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

        try:
            idx = phase_order.index(current)
            if idx < len(phase_order) - 1:
                return phase_order[idx + 1]
        except ValueError:
            pass

        return None

    # =========================================================================
    # PHASE IMPLEMENTATIONS
    # =========================================================================

    async def _phase_init(self) -> bool:
        """Initialize pipeline - load configurations and seed URLs."""
        self.log.info("Phase: INIT - Loading configurations")

        for assoc_code in self.state.association_codes:
            config = self.associations_config.get("associations", {}).get(assoc_code)
            if config:
                # Add seed URL to queue
                url = config.get("url") or config.get("directory_url")
                if url:
                    self.state.add_to_queue(
                        url=url,
                        priority=10 if config.get("priority") == "high" else 5,
                        association=assoc_code
                    )

        self.state_manager.checkpoint(self.state)
        return True

    async def _phase_gatekeeper(self) -> bool:
        """Check access permissions for all queued URLs."""
        self.log.info("Phase: GATEKEEPER - Checking access permissions")

        # Get unique domains
        domains = set()
        for item in self.state.crawl_queue:
            from urllib.parse import urlparse
            domain = urlparse(item.get("url", "")).netloc
            if domain:
                domains.add(domain)

        # Check each domain
        for domain in domains:
            result = await self.spawner.spawn(
                "discovery.access_gatekeeper",
                {"domain": domain, "check_page": False}
            )

            if not result.get("is_allowed"):
                # Block all URLs from this domain
                blocked_urls = [
                    item["url"] for item in self.state.crawl_queue
                    if domain in item.get("url", "")
                ]
                for url in blocked_urls:
                    self.state.mark_blocked(url, result.get("reasons", []))

        self.state_manager.checkpoint(self.state)
        return True

    async def _phase_discovery(self) -> bool:
        """Discover member URLs from association sites."""
        self.log.info("Phase: DISCOVERY - Finding member URLs")

        while self.state.crawl_queue:
            item = self.state.get_next_url()
            if not item:
                break

            url = item.get("url")
            if url in self.state.visited_urls or url in self.state.blocked_urls:
                continue

            # Run site mapper
            result = await self.spawner.spawn(
                "discovery.site_mapper",
                {
                    "base_url": url,
                    "association": item.get("association")
                }
            )

            self.state.mark_visited(url)

            if result.get("success"):
                # Run link crawler on directory
                directory_url = result.get("directory_url")
                if directory_url:
                    crawl_result = await self.spawner.spawn(
                        "discovery.link_crawler",
                        {
                            "entry_url": directory_url,
                            "pagination": result.get("pagination"),
                            "association": item.get("association")
                        }
                    )

                    # Add discovered URLs to queue
                    for member_url in crawl_result.get("member_urls", []):
                        self.state.add_to_queue(
                            url=member_url,
                            association=item.get("association"),
                            page_type_hint="MEMBER_DETAIL"
                        )

            # Checkpoint periodically
            if len(self.state.visited_urls) % 10 == 0:
                self.state_manager.checkpoint(self.state)

        self.state_manager.checkpoint(self.state)
        return True

    async def _phase_classification(self) -> bool:
        """Classify discovered pages by type."""
        self.log.info("Phase: CLASSIFICATION - Classifying pages")

        # Classify pages in queue
        pages_to_classify = [
            item for item in self.state.crawl_queue
            if not item.get("page_type_hint")
        ]

        for item in pages_to_classify[:100]:  # Limit batch size
            result = await self.spawner.spawn(
                "discovery.page_classifier",
                {"url": item.get("url"), "fetch": True}
            )

            if result.get("success"):
                item["page_type"] = result.get("page_type")
                item["extractor"] = result.get("recommended_extractor")

        self.state_manager.checkpoint(self.state)
        return True

    async def _phase_extraction(self) -> bool:
        """Extract data from classified pages."""
        self.log.info("Phase: EXTRACTION - Extracting data")

        while self.state.crawl_queue:
            item = self.state.get_next_url()
            if not item:
                break

            url = item.get("url")
            page_type = item.get("page_type") or item.get("page_type_hint", "MEMBER_DETAIL")
            extractor = item.get("extractor", "extraction.html_parser")

            # Select extractor based on page type
            if page_type in ["EVENTS_LIST", "EVENT_DETAIL"]:
                extractor = "extraction.event_extractor"
            elif page_type in ["SPONSORS_LIST", "EXHIBITORS_LIST", "PARTICIPANTS_LIST"]:
                extractor = "extraction.event_participant_extractor"

            # Run extraction
            result = await self.spawner.spawn(
                extractor,
                {
                    "url": url,
                    "association": item.get("association"),
                    "page_type": page_type
                }
            )

            self.state.mark_visited(url)

            if result.get("success"):
                records = result.get("records", [])

                # Store based on type
                if "event" in extractor:
                    if "participant" in extractor:
                        for rec in records:
                            self.state.add_participant(rec)
                    else:
                        for rec in records:
                            self.state.add_event(rec)
                else:
                    for rec in records:
                        self.state.add_company(rec)

            # Checkpoint periodically
            if len(self.state.visited_urls) % 50 == 0:
                self.state_manager.checkpoint(self.state)

        self.state_manager.checkpoint(self.state)
        return True

    async def _phase_enrichment(self) -> bool:
        """Enrich extracted company records."""
        self.log.info("Phase: ENRICHMENT - Enriching records")

        if not self.state.companies:
            return True

        # Run firmographic enrichment
        result = await self.spawner.spawn(
            "enrichment.firmographic",
            {"records": self.state.companies}
        )
        if result.get("success"):
            self.state.companies = result.get("records", self.state.companies)

        # Run tech stack detection
        result = await self.spawner.spawn(
            "enrichment.tech_stack",
            {"records": self.state.companies}
        )
        if result.get("success"):
            self.state.companies = result.get("records", self.state.companies)

        # Run contact finder
        result = await self.spawner.spawn(
            "enrichment.contact_finder",
            {"records": self.state.companies}
        )
        if result.get("success"):
            self.state.companies = result.get("records", self.state.companies)

        self.state_manager.checkpoint(self.state)
        return True

    async def _phase_validation(self) -> bool:
        """Validate and score records."""
        self.log.info("Phase: VALIDATION - Validating records")

        if not self.state.companies:
            return True

        # Run deduplication
        result = await self.spawner.spawn(
            "validation.dedupe",
            {"records": self.state.companies}
        )
        if result.get("success"):
            self.state.companies = result.get("records", self.state.companies)

        # Run cross-reference validation
        result = await self.spawner.spawn(
            "validation.crossref",
            {"records": self.state.companies}
        )
        if result.get("success"):
            self.state.companies = result.get("records", self.state.companies)

        # Run quality scoring
        result = await self.spawner.spawn(
            "validation.scorer",
            {"records": self.state.companies}
        )
        if result.get("success"):
            self.state.companies = result.get("records", self.state.companies)

        self.state_manager.checkpoint(self.state)
        return True

    async def _phase_resolution(self) -> bool:
        """Resolve entities to canonical form."""
        self.log.info("Phase: RESOLUTION - Resolving entities")

        if not self.state.companies:
            return True

        # Run entity resolution
        result = await self.spawner.spawn(
            "validation.entity_resolver",
            {
                "records": self.state.companies,
                "merge_strategy": "keep_best"
            }
        )

        if result.get("success"):
            self.state.canonical_entities = result.get("canonical_entities", [])
            self.state.total_entities_resolved = len(self.state.canonical_entities)

        self.state_manager.checkpoint(self.state)
        return True

    async def _phase_graph(self) -> bool:
        """Build relationship graph."""
        self.log.info("Phase: GRAPH - Building relationship graph")

        # Mine competitor signals
        for company in self.state.companies[:100]:  # Limit for performance
            if company.get("website"):
                result = await self.spawner.spawn(
                    "intelligence.competitor_signal_miner",
                    {
                        "url": company.get("website"),
                        "source_company_id": company.get("id"),
                        "association": company.get("associations", [None])[0]
                    }
                )
                if result.get("success"):
                    for signal in result.get("signals", []):
                        self.state.add_signal(signal)

        # Build graph
        result = await self.spawner.spawn(
            "intelligence.relationship_graph_builder",
            {
                "action": "build",
                "companies": self.state.canonical_entities or self.state.companies,
                "events": self.state.events,
                "participants": self.state.participants,
                "signals": self.state.competitor_signals,
                "associations": [
                    {"code": code}
                    for code in self.state.association_codes
                ]
            }
        )

        if result.get("success"):
            self.state.graph_edges = result.get("edges_created", 0)

        self.state_manager.checkpoint(self.state)
        return True

    async def _phase_export(self) -> bool:
        """Generate exports."""
        self.log.info("Phase: EXPORT - Generating exports")

        if self.dry_run:
            self.log.info("Dry run - skipping exports")
            return True

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Export companies
        if self.state.canonical_entities or self.state.companies:
            result = await self.spawner.spawn(
                "export.export_activation",
                {
                    "export_type": "companies",
                    "format": "csv",
                    "records": self.state.canonical_entities or self.state.companies,
                    "filters": {"min_quality": 60}
                }
            )
            if result.get("success"):
                self.state.add_export({
                    "type": "companies",
                    "path": result.get("export_path"),
                    "count": result.get("records_exported")
                })

        # Export events
        if self.state.events:
            result = await self.spawner.spawn(
                "export.export_activation",
                {
                    "export_type": "events",
                    "format": "csv",
                    "records": self.state.events
                }
            )
            if result.get("success"):
                self.state.add_export({
                    "type": "events",
                    "path": result.get("export_path"),
                    "count": result.get("records_exported")
                })

        # Generate summary report
        result = await self.spawner.spawn(
            "export.export_activation",
            {
                "export_type": "summary",
                "action": "summary_report",
                "companies": self.state.canonical_entities or self.state.companies,
                "events": self.state.events,
                "signals": self.state.competitor_signals
            }
        )

        self.state_manager.checkpoint(self.state)
        return True

    async def _phase_monitor(self) -> bool:
        """Create monitoring baselines."""
        self.log.info("Phase: MONITOR - Creating source baselines")

        # Create baselines for discovered directories
        directory_urls = [
            url for url in self.state.visited_urls
            if "/member" in url or "/directory" in url
        ]

        if directory_urls:
            result = await self.spawner.spawn(
                "monitoring.source_monitor",
                {
                    "action": "baseline",
                    "urls": directory_urls[:20]  # Limit
                }
            )

        self.state_manager.checkpoint(self.state)
        return True

    def _build_final_result(self) -> dict:
        """Build final result from pipeline state."""
        summary = self.state.get_summary()

        result = {
            "success": self.state.current_phase == PipelinePhase.DONE,
            "job_id": self.state.job_id,
            "final_phase": self.state.current_phase.value,
            "associations": self.state.association_codes,
            "totals": {
                "companies_extracted": self.state.total_companies_extracted,
                "events_extracted": self.state.total_events_extracted,
                "participants_extracted": self.state.total_participants_extracted,
                "signals_detected": self.state.total_signals_detected,
                "entities_resolved": self.state.total_entities_resolved,
            },
            "exports": self.state.exports,
            "errors": self.state.errors,
            "phase_history": self.state.phase_history,
            "started_at": self.state.created_at.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "records_processed": summary.get("visited_urls", 0)
        }

        # Save final results
        if not self.dry_run:
            output_path = Path(f"data/validated/{self.state.job_id}")
            output_path.mkdir(parents=True, exist_ok=True)

            with open(output_path / "pipeline_result.json", "w") as f:
                json.dump(result, f, indent=2, default=str)

            # Save companies
            self.save_records(
                self.state.canonical_entities or self.state.companies,
                str(output_path / "companies.jsonl")
            )

        return result

    # =========================================================================
    # LEGACY METHODS (for backward compatibility)
    # =========================================================================

    async def _run_extraction(self, associations: list[str]) -> dict:
        """Run extraction for specified associations (legacy mode)."""
        self.log.info("Starting extraction phase", associations=associations)

        if not associations:
            associations = [
                code for code, config in self.associations_config.get("associations", {}).items()
                if config.get("priority") == "high"
            ]

        results = {
            "phase": "extraction",
            "associations": {},
            "total_records": 0,
            "success": True
        }

        for association in associations:
            self.log.info(f"Extracting {association}")

            await self.checkpoint({
                "phase": "extraction",
                "current_association": association,
                "completed_associations": list(results["associations"].keys())
            })

            try:
                assoc_result = await self._extract_association(association)
                results["associations"][association] = assoc_result
                results["total_records"] += assoc_result.get("records_extracted", 0)
            except Exception as e:
                self.log.error(f"Failed to extract {association}: {e}")
                results["associations"][association] = {
                    "success": False,
                    "error": str(e)
                }
                results["success"] = False

        # Compute aggregate metrics
        associations_failed = sum(
            1 for r in results["associations"].values() if not r.get("success")
        )
        total_attempted = len(associations)
        aggregate_error_rate = associations_failed / total_attempted if total_attempted else 0

        results["total_errors"] = associations_failed
        results["error_rate"] = aggregate_error_rate
        results["associations_failed"] = associations_failed

        # Mark overall failure if aggregate error rate exceeds threshold
        if aggregate_error_rate > self.max_error_rate:
            results["success"] = False
            results["error"] = (
                f"Aggregate error rate {aggregate_error_rate:.0%} exceeds threshold"
            )

        self.log.info(
            "Extraction phase complete",
            total_records=results["total_records"],
            associations_processed=len(associations),
            associations_failed=associations_failed,
            error_rate=f"{aggregate_error_rate:.0%}",
        )

        return results

    async def _extract_association(self, association: str) -> dict:
        """Extract all members from a single association."""
        config = self.associations_config.get("associations", {}).get(association)

        if not config:
            raise ValueError(f"Association {association} not found in configuration")

        extraction_mode = config.get("extraction_mode", "standard")

        # District-based extraction: skip discovery, directly parse each district page
        if extraction_mode == "district_directories":
            return await self._extract_district_directories(association, config)

        # Standard extraction flow
        # Step 1: Discover URLs
        self.log.info(f"Mapping {association} site")

        site_map_result = await self.spawner.spawn(
            "discovery.site_mapper",
            {
                "base_url": config["url"],
                "directory_url": config.get("directory_url"),
                "association": association
            }
        )

        if not site_map_result.get("success"):
            return site_map_result

        # Step 2: Crawl for all member URLs
        self.log.info(f"Crawling {association} directory")

        crawl_result = await self.spawner.spawn(
            "discovery.link_crawler",
            {
                "entry_url": site_map_result.get("directory_url", config.get("directory_url")),
                "pagination": site_map_result.get("pagination", config.get("pagination")),
                "association": association
            }
        )

        if not crawl_result.get("success"):
            return crawl_result

        member_urls = crawl_result.get("member_urls", [])
        self.log.info(f"Found {len(member_urls)} member URLs for {association}")

        # Step 3: Extract data from each URL
        schema = config.get("schema", "default")
        all_records = []

        if not member_urls:
            # Fallback: no individual member pages found, try directory parser
            # This handles sites like SOCMA that list all members inline
            directory_url = site_map_result.get("directory_url", config.get("directory_url"))
            if directory_url:
                self.log.info(f"No member URLs found, trying directory extraction for {association}")
                dir_result = await self.spawner.spawn(
                    "extraction.directory_parser",
                    {
                        "url": directory_url,
                        "schema": schema,
                        "association": association
                    }
                )
                if dir_result.get("success") and dir_result.get("records"):
                    all_records.extend(dir_result["records"])
        else:
            batch_size = 100
            url_batches = [
                member_urls[i:i + batch_size]
                for i in range(0, len(member_urls), batch_size)
            ]

            for batch_num, url_batch in enumerate(url_batches):
                self.log.info(f"Processing batch {batch_num + 1}/{len(url_batches)}")

                tasks = [
                    {"url": url, "schema": schema, "association": association}
                    for url in url_batch
                ]

                batch_results = await self.spawner.spawn_parallel(
                    "extraction.html_parser",
                    tasks,
                    max_concurrent=5
                )

                for result in batch_results:
                    if result.get("success") and result.get("records"):
                        all_records.extend(result["records"])

        output_path = f"data/raw/{association}/records_{self.job_id}.jsonl"

        if not self.dry_run:
            self.save_records(all_records, output_path)

        return {
            "success": True,
            "association": association,
            "urls_discovered": len(member_urls),
            "records_extracted": len(all_records),
            "output_path": output_path
        }

    async def _extract_district_directories(self, association: str, config: dict) -> dict:
        """
        Extract members from multiple district directory pages.

        Used for associations like PMA where members are listed on separate
        per-district pages rather than a single searchable directory.
        """
        district_urls = config.get("district_urls", [])
        schema = config.get("schema", "default")

        if not district_urls:
            return {
                "success": False,
                "error": f"No district_urls configured for {association}",
                "records_extracted": 0
            }

        self.log.info(
            f"Extracting {association} from {len(district_urls)} district pages"
        )

        all_records = []

        # Process each district page through the DirectoryParser
        tasks = [
            {"url": url, "schema": schema, "association": association}
            for url in district_urls
        ]

        results = await self.spawner.spawn_parallel(
            "extraction.directory_parser",
            tasks,
            max_concurrent=3  # Conservative to avoid WAF blocks
        )

        successes = sum(1 for r in results if r.get("success"))
        failures = len(results) - successes
        error_rate = failures / len(results) if results else 0

        for result in results:
            if result.get("success") and result.get("records"):
                all_records.extend(result["records"])

        if error_rate > self.max_error_rate:
            self.log.error(
                f"Error rate {error_rate:.0%} exceeds threshold {self.max_error_rate:.0%}"
            )
            return {
                "success": False,
                "error": f"Error rate {error_rate:.0%} exceeds threshold",
                "association": association,
                "urls_discovered": len(district_urls),
                "records_extracted": len(all_records),
                "successes": successes,
                "failures": failures,
                "error_rate": error_rate,
            }

        # Deduplicate by member_id or company_name (members can appear in multiple districts)
        seen = set()
        unique_records = []
        for record in all_records:
            key = record.get("member_id") or record.get("company_name", "")
            if key and key not in seen:
                seen.add(key)
                unique_records.append(record)

        output_path = f"data/raw/{association}/records_{self.job_id}.jsonl"

        if not self.dry_run:
            self.save_records(unique_records, output_path)

        self.log.info(
            f"Extracted {len(unique_records)} unique records from {len(district_urls)} districts "
            f"({len(all_records)} total before dedup)"
        )

        return {
            "success": True,
            "association": association,
            "urls_discovered": len(district_urls),
            "records_extracted": len(unique_records),
            "records_before_dedup": len(all_records),
            "output_path": output_path
        }

    async def _run_enrichment(self, enrichment_type: str) -> dict:
        """Run enrichment on processed records."""
        self.log.info(f"Starting enrichment phase: {enrichment_type}")

        raw_records = self._load_all_raw_records()

        if not raw_records:
            return {
                "phase": "enrichment",
                "success": False,
                "error": "No raw records found to enrich"
            }

        self.log.info(f"Loaded {len(raw_records)} records to enrich")

        enriched_records = raw_records

        if enrichment_type in ["all", "firmographic"]:
            self.log.info("Running firmographic enrichment")
            result = await self.spawner.spawn(
                "enrichment.firmographic",
                {"records": enriched_records}
            )
            if result.get("success"):
                enriched_records = result.get("records", enriched_records)

        if enrichment_type in ["all", "techstack"]:
            self.log.info("Running tech stack detection")
            result = await self.spawner.spawn(
                "enrichment.tech_stack",
                {"records": enriched_records}
            )
            if result.get("success"):
                enriched_records = result.get("records", enriched_records)

        if enrichment_type in ["all", "contacts"]:
            self.log.info("Running contact finder")
            result = await self.spawner.spawn(
                "enrichment.contact_finder",
                {"records": enriched_records}
            )
            if result.get("success"):
                enriched_records = result.get("records", enriched_records)

        output_path = f"data/processed/enriched_{self.job_id}.jsonl"

        if not self.dry_run:
            self.save_records(enriched_records, output_path)

        return {
            "phase": "enrichment",
            "success": True,
            "records_input": len(raw_records),
            "records_output": len(enriched_records),
            "output_path": output_path
        }

    async def _run_validation(self, validation_type: str) -> dict:
        """Run validation on enriched records."""
        self.log.info(f"Starting validation phase: {validation_type}")

        processed_records = self._load_processed_records()

        if not processed_records:
            return {
                "phase": "validation",
                "success": False,
                "error": "No processed records found to validate"
            }

        self.log.info(f"Loaded {len(processed_records)} records to validate")

        validated_records = processed_records

        if validation_type in ["all", "dedupe"]:
            self.log.info("Running duplicate detection")
            result = await self.spawner.spawn(
                "validation.dedupe",
                {"records": validated_records}
            )
            if result.get("success"):
                validated_records = result.get("records", validated_records)
                self.log.info(f"After deduplication: {len(validated_records)} records")

        if validation_type in ["all", "crossref"]:
            self.log.info("Running cross-reference validation")
            result = await self.spawner.spawn(
                "validation.crossref",
                {"records": validated_records}
            )
            if result.get("success"):
                validated_records = result.get("records", validated_records)

        if validation_type in ["all", "score"]:
            self.log.info("Running quality scoring")
            result = await self.spawner.spawn(
                "validation.scorer",
                {"records": validated_records}
            )
            if result.get("success"):
                validated_records = result.get("records", validated_records)

        min_score = self.agent_config.get("min_quality_score", 60)
        final_records = [
            r for r in validated_records
            if r.get("quality_score", 0) >= min_score
        ]

        self.log.info(
            f"Quality filter: {len(validated_records)} → {len(final_records)} records "
            f"(min score: {min_score})"
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"data/validated/{timestamp}/companies.jsonl"

        if not self.dry_run:
            self.save_records(final_records, output_path)

            summary = self._generate_summary(final_records)
            summary_path = f"data/validated/{timestamp}/summary.json"
            Path(summary_path).parent.mkdir(parents=True, exist_ok=True)
            with open(summary_path, "w") as f:
                json.dump(summary, f, indent=2)

        return {
            "phase": "validation",
            "success": True,
            "records_input": len(processed_records),
            "records_after_dedupe": len(validated_records),
            "records_final": len(final_records),
            "output_path": output_path
        }

    def _load_all_raw_records(self) -> list[dict]:
        """Load all raw records from data/raw/*/."""
        records = []
        raw_path = Path("data/raw")

        if not raw_path.exists():
            return records

        for jsonl_file in raw_path.glob("**/*.jsonl"):
            try:
                file_records = self.load_records(str(jsonl_file))
                records.extend(file_records)
            except Exception as e:
                self.log.warning(f"Failed to load {jsonl_file}: {e}")

        return records

    def _load_processed_records(self) -> list[dict]:
        """Load processed records from data/processed/."""
        records = []
        processed_path = Path("data/processed")

        if not processed_path.exists():
            return records

        jsonl_files = list(processed_path.glob("enriched_*.jsonl"))

        if jsonl_files:
            latest = max(jsonl_files, key=lambda p: p.stat().st_mtime)
            records = self.load_records(str(latest))

        return records

    def _generate_summary(self, records: list[dict]) -> dict:
        """Generate summary statistics for validated records."""
        if not records:
            return {"total_records": 0}

        quality_dist = {"A": 0, "B": 0, "C": 0, "D": 0, "F": 0}
        for r in records:
            grade = r.get("quality_grade", "F")
            quality_dist[grade] = quality_dist.get(grade, 0) + 1

        fields_to_check = [
            "company_name", "website", "city", "state",
            "employee_count_min", "erp_system", "contacts"
        ]
        completeness = {}
        for field in fields_to_check:
            filled = sum(1 for r in records if r.get(field))
            completeness[field] = round(filled / len(records), 2)

        erp_dist = {}
        for r in records:
            erp = r.get("erp_system", "Unknown")
            erp_dist[erp] = erp_dist.get(erp, 0) + 1

        associations = {}
        for r in records:
            for assoc in r.get("associations", [r.get("association", "Unknown")]):
                associations[assoc] = associations.get(assoc, 0) + 1

        scores = [r.get("quality_score", 0) for r in records]

        return {
            "total_records": len(records),
            "quality_distribution": quality_dist,
            "average_quality_score": round(sum(scores) / len(scores), 1),
            "median_quality_score": sorted(scores)[len(scores) // 2],
            "field_completeness": completeness,
            "erp_distribution": dict(sorted(erp_dist.items(), key=lambda x: -x[1])[:10]),
            "associations": associations,
            "generated_at": datetime.now(UTC).isoformat()
        }


# CLI Interface
@click.command()
@click.option("--mode", type=click.Choice([
    "extract", "extract-all", "enrich", "enrich-all",
    "validate", "validate-all", "full"
]), default="full", help="Pipeline mode")
@click.option("--associations", "-a", multiple=True, help="Association codes to process")
@click.option("--enrichment", type=click.Choice([
    "firmographic", "techstack", "contacts", "all"
]), default="all", help="Enrichment type (for enrich mode)")
@click.option("--validation", type=click.Choice([
    "dedupe", "crossref", "score", "all"
]), default="all", help="Validation type (for validate mode)")
@click.option("--dry-run", is_flag=True, help="Run without saving results")
@click.option("--job-id", help="Specific job ID (for resume)")
@click.option("--resume", help="Resume from job ID")
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    default="INFO",
    help="Logging level",
)
@click.option("--persist-db", is_flag=True, help="Also persist results to PostgreSQL")
def main(mode, associations, enrichment, validation, dry_run, job_id, resume, log_level, persist_db):
    """
    NAM Competitive Intelligence Pipeline Orchestrator

    Run data extraction, enrichment, and validation for manufacturing
    company intelligence.
    """
    job_id = job_id or str(uuid.uuid4())

    click.echo(f"Starting pipeline job: {job_id}")
    click.echo(f"Mode: {mode}")

    if associations:
        click.echo(f"Associations: {', '.join(associations)}")

    if dry_run:
        click.echo("DRY RUN - no data will be saved")

    # Optionally connect to PostgreSQL
    db_pool = None
    if persist_db:
        import os as _os
        db_url = _os.getenv("DATABASE_URL")
        if not db_url:
            click.echo(click.style("ERROR: --persist-db requires DATABASE_URL env var", fg="red"))
            raise SystemExit(1)
        click.echo("Database persistence enabled")

        from db.connection import DatabasePool
        db_pool = DatabasePool(db_url)
        asyncio.run(db_pool.init())

    import logging as _logging

    orchestrator = OrchestratorAgent(
        agent_type="orchestrator",
        job_id=job_id,
        db_pool=db_pool,
        mode=mode,
        associations=list(associations),
        dry_run=dry_run,
    )
    orchestrator.log.logger.setLevel(getattr(_logging, log_level.upper()))

    task = {
        "mode": mode,
        "associations": list(associations),
        "enrichment": enrichment,
        "validation": validation,
        "resume_job_id": resume
    }

    result = asyncio.run(orchestrator.execute(task))

    # Cleanup DB pool
    if db_pool is not None:
        asyncio.run(db_pool.close())

    if result.get("success"):
        click.echo(click.style("\n[OK] Pipeline completed successfully", fg="green"))

        if "final_record_count" in result:
            click.echo(f"Final records: {result['final_record_count']}")

        if result.get("totals"):
            click.echo(f"Companies: {result['totals'].get('companies_extracted', 0)}")
            click.echo(f"Events: {result['totals'].get('events_extracted', 0)}")
            click.echo(f"Resolved entities: {result['totals'].get('entities_resolved', 0)}")

        if result.get("output_path"):
            click.echo(f"Output: {result['output_path']}")
    else:
        click.echo(click.style("\n[FAIL] Pipeline failed", fg="red"))
        click.echo(f"Error: {result.get('error', 'Unknown error')}")
        if result.get("errors"):
            for err in result["errors"][:5]:
                click.echo(f"  - {err.get('error_message', err)}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
