"""
Export Activation Agent
NAM Intelligence Pipeline

Generates marketing-ready exports from pipeline data with
provenance tracking and CRM formatting.
"""

import csv
import json
from datetime import UTC, datetime
from pathlib import Path

from agents.base import BaseAgent
from middleware.policy import validate_json_output


class ExportActivationAgent(BaseAgent):
    """
    Export Activation Agent - generates marketing-ready exports.

    Responsibilities:
    - Generate CSV/JSON exports with provenance
    - Map fields for CRM integration (Salesforce, HubSpot)
    - Create competitor intelligence reports
    - Filter by quality thresholds
    """

    # CRM field mappings
    SALESFORCE_MAPPING = {
        "company_name": "Account Name",
        "website": "Website",
        "domain": "Domain__c",
        "city": "BillingCity",
        "state": "BillingState",
        "country": "BillingCountry",
        "employee_count_min": "NumberOfEmployees",
        "revenue_min_usd": "AnnualRevenue",
        "industry": "Industry",
        "erp_system": "ERP_System__c",
        "associations": "Associations__c",
        "quality_score": "Data_Quality_Score__c",
    }

    HUBSPOT_MAPPING = {
        "company_name": "name",
        "website": "website",
        "domain": "domain",
        "city": "city",
        "state": "state",
        "country": "country",
        "employee_count_min": "numberofemployees",
        "revenue_min_usd": "annualrevenue",
        "industry": "industry",
        "erp_system": "erp_system",
    }

    def _setup(self, **kwargs):
        """Initialize export settings."""
        self.export_dir = Path(self.agent_config.get("export_dir", "data/exports"))
        self.export_dir.mkdir(parents=True, exist_ok=True)
        self.min_quality = self.agent_config.get("min_quality_score", 60)

    @validate_json_output
    async def run(self, task: dict) -> dict:
        """
        Generate exports from pipeline data.

        Args:
            task: {
                "export_type": "companies" | "events" | "participants" | "competitors",
                "format": "csv" | "json" | "salesforce" | "hubspot",
                "records": [{...}, ...],
                "filters": {
                    "min_quality": 70,
                    "associations": ["PMA", "NEMA"],
                    "has_contacts": True
                },
                "output_path": "path/to/output.csv"  # Optional
            }

        Returns:
            {
                "success": True,
                "export_path": "data/exports/...",
                "records_exported": 100,
                "format": "csv"
            }
        """
        export_type = task.get("export_type", "companies")
        export_format = task.get("format", "csv")
        records = task.get("records", [])
        filters = task.get("filters", {})
        output_path = task.get("output_path")

        if not records:
            return {
                "success": False,
                "error": "No records provided",
                "records_processed": 0
            }

        self.log.info(
            f"Generating {export_format} export for {export_type}",
            input_records=len(records)
        )

        # Apply filters
        filtered_records = self._apply_filters(records, filters)

        self.log.info(
            f"After filtering: {len(filtered_records)} records",
            original=len(records)
        )

        # Generate timestamp for filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Generate export based on format
        if export_format == "csv":
            export_path = output_path or str(
                self.export_dir / f"{export_type}_{timestamp}.csv"
            )
            self._export_csv(filtered_records, export_path, export_type)

        elif export_format == "json":
            export_path = output_path or str(
                self.export_dir / f"{export_type}_{timestamp}.json"
            )
            self._export_json(filtered_records, export_path, export_type)

        elif export_format == "salesforce":
            export_path = output_path or str(
                self.export_dir / f"{export_type}_salesforce_{timestamp}.csv"
            )
            self._export_crm(filtered_records, export_path, self.SALESFORCE_MAPPING)

        elif export_format == "hubspot":
            export_path = output_path or str(
                self.export_dir / f"{export_type}_hubspot_{timestamp}.csv"
            )
            self._export_crm(filtered_records, export_path, self.HUBSPOT_MAPPING)

        else:
            return {
                "success": False,
                "error": f"Unknown format: {export_format}",
                "records_processed": 0
            }

        self.log.info(
            f"Export complete: {export_path}",
            records_exported=len(filtered_records)
        )

        return {
            "success": True,
            "export_path": export_path,
            "records_exported": len(filtered_records),
            "format": export_format,
            "records_processed": len(records)
        }

    def _apply_filters(self, records: list[dict], filters: dict) -> list[dict]:
        """Apply filters to records."""
        if not filters:
            return records

        filtered = []

        min_quality = filters.get("min_quality", self.min_quality)
        target_associations = filters.get("associations", [])
        has_contacts = filters.get("has_contacts", False)
        has_email = filters.get("has_email", False)
        has_erp = filters.get("has_erp", False)
        states = filters.get("states", [])
        industries = filters.get("industries", [])

        for record in records:
            # Quality filter
            quality = record.get("quality_score", 0)
            if quality < min_quality:
                continue

            # Association filter
            if target_associations:
                record_assocs = record.get("associations", [])
                if isinstance(record_assocs, str):
                    record_assocs = [record_assocs]
                if not any(a in record_assocs for a in target_associations):
                    continue

            # Contact filter
            if has_contacts:
                contacts = record.get("contacts", [])
                if not contacts:
                    continue

            # Email filter
            if has_email:
                contacts = record.get("contacts", [])
                has_email_contact = any(c.get("email") for c in contacts)
                if not has_email_contact:
                    continue

            # ERP filter
            if has_erp:
                if not record.get("erp_system"):
                    continue

            # State filter
            if states:
                record_state = record.get("state", "")
                if record_state not in states:
                    continue

            # Industry filter
            if industries:
                record_industry = record.get("industry", "")
                if record_industry not in industries:
                    continue

            filtered.append(record)

        return filtered

    def _export_csv(self, records: list[dict], path: str, export_type: str):
        """Export records to CSV."""
        if not records:
            return

        # Determine columns based on export type
        columns = self._get_columns(export_type, records)

        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()

            for record in records:
                # Flatten nested fields
                flat_record = self._flatten_record(record)
                writer.writerow(flat_record)

    def _export_json(self, records: list[dict], path: str, export_type: str):
        """Export records to JSON with provenance."""
        export_data = {
            "export_type": export_type,
            "exported_at": datetime.now(UTC).isoformat(),
            "record_count": len(records),
            "records": records,
            "provenance": {
                "pipeline": "NAM Intelligence Pipeline",
                "job_id": self.job_id,
                "agent": self.agent_type,
            }
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(export_data, f, indent=2, default=str)

    def _export_crm(self, records: list[dict], path: str, mapping: dict):
        """Export records with CRM field mapping."""
        if not records:
            return

        # Map fields
        mapped_records = []
        for record in records:
            flat_record = self._flatten_record(record)
            mapped = {}

            for source_field, target_field in mapping.items():
                value = flat_record.get(source_field)
                if value is not None:
                    # Handle special formatting
                    if isinstance(value, list):
                        value = "; ".join(str(v) for v in value)
                    mapped[target_field] = value

            # Add provenance fields
            mapped["Data_Source__c"] = "NAM Intelligence Pipeline"
            mapped["Import_Date__c"] = datetime.now(UTC).strftime("%Y-%m-%d")

            mapped_records.append(mapped)

        # Write CSV with mapped columns
        if mapped_records:
            columns = list(mapped_records[0].keys())

            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=columns)
                writer.writeheader()
                writer.writerows(mapped_records)

    def _get_columns(self, export_type: str, records: list[dict]) -> list[str]:
        """Determine columns for export type."""
        if export_type == "companies":
            return [
                "company_name", "website", "domain", "city", "state", "country",
                "employee_count_min", "employee_count_max", "revenue_min_usd",
                "industry", "naics_code", "erp_system", "crm_system",
                "associations", "quality_score", "quality_grade",
                "contact_count", "primary_contact_name", "primary_contact_email",
                "source_url", "extracted_at"
            ]
        elif export_type == "events":
            return [
                "title", "event_type", "start_date", "end_date",
                "venue", "city", "state", "country", "is_virtual",
                "event_url", "registration_url",
                "organizer_association", "sponsor_count", "exhibitor_count",
                "source_url", "extracted_at"
            ]
        elif export_type == "participants":
            return [
                "company_name", "company_website", "participant_type",
                "sponsor_tier", "booth_number", "booth_category",
                "speaker_name", "speaker_title", "presentation_title",
                "event_id", "source_url", "extracted_at"
            ]
        elif export_type == "competitors":
            return [
                "competitor_name", "signal_type", "confidence",
                "context", "source_company_id", "source_event_id",
                "source_association", "source_url", "detected_at"
            ]
        else:
            # Auto-detect from records
            if records:
                return list(records[0].keys())
            return []

    def _flatten_record(self, record: dict) -> dict:
        """Flatten nested record fields for CSV export."""
        flat = {}

        for key, value in record.items():
            if key in ["provenance", "_meta"]:
                continue

            if isinstance(value, list):
                if key == "contacts" and value:
                    # Extract primary contact
                    flat["contact_count"] = len(value)
                    primary = value[0]
                    flat["primary_contact_name"] = primary.get("full_name", "")
                    flat["primary_contact_email"] = primary.get("email", "")
                    flat["primary_contact_title"] = primary.get("title", "")
                elif key == "associations":
                    flat[key] = "; ".join(str(v) for v in value)
                elif key == "tech_stack":
                    flat[key] = "; ".join(str(v) for v in value)
                else:
                    flat[key] = json.dumps(value)

            elif isinstance(value, dict):
                # Flatten nested dicts
                for sub_key, sub_value in value.items():
                    flat[f"{key}_{sub_key}"] = sub_value

            elif isinstance(value, datetime):
                flat[key] = value.isoformat()

            else:
                flat[key] = value

        return flat

    async def generate_competitor_report(self, task: dict) -> dict:
        """
        Generate a competitor intelligence report.

        Args:
            task: {
                "signals": [{CompetitorSignal}, ...],
                "output_path": "path/to/report.json"
            }
        """
        signals = task.get("signals", [])
        output_path = task.get("output_path")

        if not signals:
            return {
                "success": False,
                "error": "No signals provided",
                "records_processed": 0
            }

        # Build report
        from agents.intelligence.competitor_signal_miner import CompetitorReportGenerator
        report = CompetitorReportGenerator.generate_report(signals)

        # Add metadata
        report["report_type"] = "Competitor Intelligence"
        report["pipeline"] = "NAM Intelligence Pipeline"
        report["job_id"] = self.job_id

        # Save report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_path = output_path or str(
            self.export_dir / f"competitor_report_{timestamp}.json"
        )

        with open(export_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)

        self.log.info(f"Competitor report generated: {export_path}")

        return {
            "success": True,
            "export_path": export_path,
            "competitors_analyzed": len(report.get("competitors", {})),
            "total_signals": report.get("total_signals", 0),
            "records_processed": len(signals)
        }

    async def generate_summary_report(self, task: dict) -> dict:
        """
        Generate a pipeline summary report.

        Args:
            task: {
                "companies": [...],
                "events": [...],
                "signals": [...],
                "output_path": "path/to/summary.json"
            }
        """
        companies = task.get("companies", [])
        events = task.get("events", [])
        signals = task.get("signals", [])
        output_path = task.get("output_path")

        # Build summary
        summary = {
            "report_type": "Pipeline Summary",
            "generated_at": datetime.now(UTC).isoformat(),
            "pipeline": "NAM Intelligence Pipeline",
            "job_id": self.job_id,
            "totals": {
                "companies": len(companies),
                "events": len(events),
                "competitor_signals": len(signals),
            },
            "company_stats": self._compute_company_stats(companies),
            "event_stats": self._compute_event_stats(events),
            "signal_stats": self._compute_signal_stats(signals),
        }

        # Save report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        export_path = output_path or str(
            self.export_dir / f"pipeline_summary_{timestamp}.json"
        )

        with open(export_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, default=str)

        self.log.info(f"Summary report generated: {export_path}")

        return {
            "success": True,
            "export_path": export_path,
            "summary": summary,
            "records_processed": len(companies) + len(events) + len(signals)
        }

    def _compute_company_stats(self, companies: list[dict]) -> dict:
        """Compute company statistics."""
        if not companies:
            return {}

        # Quality distribution
        quality_dist = {"A": 0, "B": 0, "C": 0, "D": 0, "F": 0}
        for c in companies:
            grade = c.get("quality_grade", "F")
            quality_dist[grade] = quality_dist.get(grade, 0) + 1

        # Association distribution
        assoc_dist = {}
        for c in companies:
            for assoc in c.get("associations", []):
                assoc_dist[assoc] = assoc_dist.get(assoc, 0) + 1

        # State distribution
        state_dist = {}
        for c in companies:
            state = c.get("state", "Unknown")
            state_dist[state] = state_dist.get(state, 0) + 1

        # ERP distribution
        erp_dist = {}
        for c in companies:
            erp = c.get("erp_system", "Unknown")
            erp_dist[erp] = erp_dist.get(erp, 0) + 1

        scores = [c.get("quality_score", 0) for c in companies if c.get("quality_score")]

        return {
            "quality_distribution": quality_dist,
            "association_distribution": dict(sorted(assoc_dist.items(), key=lambda x: -x[1])[:10]),
            "state_distribution": dict(sorted(state_dist.items(), key=lambda x: -x[1])[:10]),
            "erp_distribution": dict(sorted(erp_dist.items(), key=lambda x: -x[1])[:10]),
            "avg_quality_score": sum(scores) / len(scores) if scores else 0,
            "with_contacts": sum(1 for c in companies if c.get("contacts")),
            "with_erp": sum(1 for c in companies if c.get("erp_system")),
        }

    def _compute_event_stats(self, events: list[dict]) -> dict:
        """Compute event statistics."""
        if not events:
            return {}

        type_dist = {}
        for e in events:
            etype = e.get("event_type", "OTHER")
            type_dist[etype] = type_dist.get(etype, 0) + 1

        return {
            "type_distribution": type_dist,
            "virtual_count": sum(1 for e in events if e.get("is_virtual")),
            "with_registration_url": sum(1 for e in events if e.get("registration_url")),
        }

    def _compute_signal_stats(self, signals: list[dict]) -> dict:
        """Compute competitor signal statistics."""
        if not signals:
            return {}

        competitor_dist = {}
        type_dist = {}

        for s in signals:
            comp = s.get("competitor_name", "Unknown")
            competitor_dist[comp] = competitor_dist.get(comp, 0) + 1

            stype = s.get("signal_type", "UNKNOWN")
            type_dist[stype] = type_dist.get(stype, 0) + 1

        return {
            "competitor_distribution": dict(sorted(competitor_dist.items(), key=lambda x: -x[1])),
            "signal_type_distribution": type_dist,
        }
