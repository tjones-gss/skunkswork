"""
Tests for ExportActivationAgent
NAM Intelligence Pipeline

Covers: initialization, filtering, CSV/JSON/CRM export, run() method,
record flattening, column selection, competitor reports, summary reports,
and stats computation.
"""

import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from middleware.secrets import _reset_secrets_manager

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def reset_secrets_singleton():
    _reset_secrets_manager()
    yield
    _reset_secrets_manager()


def create_export_agent(agent_config=None):
    from agents.export.export_activation import ExportActivationAgent

    with (
        patch("agents.base.Config") as mock_config,
        patch("agents.base.StructuredLogger"),
        patch("agents.base.AsyncHTTPClient"),
        patch("agents.base.RateLimiter"),
    ):
        mock_config.return_value.load.return_value = agent_config or {}
        agent = ExportActivationAgent(
            agent_type="export.export_activation",
            job_id="test-job-123",
        )
        return agent


@pytest.fixture
def export_agent(tmp_path):
    agent = create_export_agent({"export_dir": str(tmp_path)})
    agent.export_dir = tmp_path
    return agent


@pytest.fixture
def sample_companies():
    """Batch of company records with varied attributes."""
    return [
        {
            "company_name": "Acme Manufacturing",
            "website": "https://acme-mfg.com",
            "domain": "acme-mfg.com",
            "city": "Detroit",
            "state": "MI",
            "country": "United States",
            "employee_count_min": 250,
            "employee_count_max": 500,
            "revenue_min_usd": 50_000_000,
            "industry": "Machine Shops",
            "naics_code": "332710",
            "erp_system": "SAP",
            "crm_system": "Salesforce",
            "associations": ["PMA", "NEMA"],
            "quality_score": 90,
            "quality_grade": "A",
            "contacts": [
                {"full_name": "John Smith", "title": "CIO", "email": "jsmith@acme.com"}
            ],
            "tech_stack": ["SAP", "Salesforce", "AWS"],
        },
        {
            "company_name": "Beta Industries",
            "website": "https://beta-ind.com",
            "domain": "beta-ind.com",
            "city": "Chicago",
            "state": "IL",
            "country": "United States",
            "employee_count_min": 50,
            "revenue_min_usd": 10_000_000,
            "industry": "Welding",
            "erp_system": None,
            "associations": ["PMA"],
            "quality_score": 70,
            "quality_grade": "B",
            "contacts": [],
            "tech_stack": ["WordPress"],
        },
        {
            "company_name": "Gamma Systems",
            "website": "https://gamma.com",
            "domain": "gamma.com",
            "city": "Cleveland",
            "state": "OH",
            "country": "United States",
            "employee_count_min": 500,
            "revenue_min_usd": 100_000_000,
            "industry": "Automation",
            "erp_system": "Oracle",
            "associations": ["NEMA"],
            "quality_score": 45,
            "quality_grade": "D",
            "contacts": [
                {"full_name": "Jane Doe", "title": "CFO", "email": "jdoe@gamma.com"}
            ],
            "tech_stack": [],
        },
    ]


@pytest.fixture
def sample_events():
    return [
        {
            "title": "FABTECH 2024",
            "event_type": "TRADE_SHOW",
            "start_date": "2024-10-15",
            "end_date": "2024-10-17",
            "venue": "Orlando Convention Center",
            "city": "Orlando",
            "state": "FL",
            "country": "United States",
            "is_virtual": False,
            "event_url": "https://fabtech.com",
            "registration_url": "https://fabtech.com/register",
            "organizer_association": "SME",
        },
        {
            "title": "Webinar: Industry Trends",
            "event_type": "WEBINAR",
            "start_date": "2024-02-28",
            "is_virtual": True,
            "event_url": "https://pma.org/webinar",
            "organizer_association": "PMA",
        },
    ]


@pytest.fixture
def sample_signals():
    return [
        {
            "competitor_name": "SAP",
            "competitor_normalized": "sap",
            "signal_type": "SPONSORSHIP",
            "confidence": 0.9,
            "source_association": "PMA",
        },
        {
            "competitor_name": "SAP",
            "competitor_normalized": "sap",
            "signal_type": "EXHIBITOR",
            "confidence": 0.85,
            "source_association": "NEMA",
        },
        {
            "competitor_name": "Oracle",
            "competitor_normalized": "oracle",
            "signal_type": "SPONSORSHIP",
            "confidence": 0.8,
            "source_association": "PMA",
        },
    ]


# =========================================================================
# 1. Initialization Tests
# =========================================================================

class TestInitialization:
    """Agent construction and _setup behaviour."""

    def test_constructor_creates_agent(self, export_agent):
        assert export_agent.agent_type == "export.export_activation"
        assert export_agent.job_id == "test-job-123"

    def test_export_dir_created(self, tmp_path):
        sub = tmp_path / "nested" / "exports"
        agent = create_export_agent({"export_dir": str(sub)})
        agent._setup()  # re-invoke to use our config
        # _setup should have created the directory
        assert Path(agent.export_dir).exists() or sub.exists()

    def test_min_quality_score_default(self, export_agent):
        assert export_agent.min_quality == 60

    def test_min_quality_score_from_config(self, tmp_path):
        agent = create_export_agent({"export_dir": str(tmp_path)})
        # Simulate config-driven min_quality_score by setting agent_config
        # and re-running _setup (the mock Config doesn't nest by agent_type parts).
        agent.agent_config = {"export_dir": str(tmp_path), "min_quality_score": 80}
        agent._setup()
        assert agent.min_quality == 80


# =========================================================================
# 2. Filtering Tests
# =========================================================================

class TestApplyFilters:
    """Tests for _apply_filters."""

    def test_no_filters_returns_all(self, export_agent, sample_companies):
        result = export_agent._apply_filters(sample_companies, {})
        assert len(result) == len(sample_companies)

    def test_min_quality_filter(self, export_agent, sample_companies):
        result = export_agent._apply_filters(
            sample_companies, {"min_quality": 70}
        )
        assert all(r["quality_score"] >= 70 for r in result)
        assert len(result) == 2  # Acme (90), Beta (70)

    def test_association_filter_with_list(self, export_agent, sample_companies):
        result = export_agent._apply_filters(
            sample_companies, {"associations": ["NEMA"], "min_quality": 0}
        )
        names = [r["company_name"] for r in result]
        assert "Acme Manufacturing" in names  # PMA + NEMA
        assert "Gamma Systems" in names       # NEMA
        assert "Beta Industries" not in names  # PMA only

    def test_association_filter_with_string_record(self, export_agent):
        """When a record stores associations as a plain string."""
        records = [
            {"company_name": "X", "associations": "PMA", "quality_score": 80},
            {"company_name": "Y", "associations": "NEMA", "quality_score": 80},
        ]
        result = export_agent._apply_filters(
            records, {"associations": ["PMA"], "min_quality": 0}
        )
        assert len(result) == 1
        assert result[0]["company_name"] == "X"

    def test_has_contacts_filter(self, export_agent, sample_companies):
        result = export_agent._apply_filters(
            sample_companies, {"has_contacts": True, "min_quality": 0}
        )
        assert len(result) == 2  # Acme and Gamma have contacts
        names = {r["company_name"] for r in result}
        assert "Beta Industries" not in names

    def test_has_email_filter(self, export_agent, sample_companies):
        result = export_agent._apply_filters(
            sample_companies, {"has_email": True, "min_quality": 0}
        )
        # Both Acme and Gamma contacts have email addresses
        assert len(result) == 2

    def test_has_erp_filter(self, export_agent, sample_companies):
        result = export_agent._apply_filters(
            sample_companies, {"has_erp": True, "min_quality": 0}
        )
        names = [r["company_name"] for r in result]
        assert "Acme Manufacturing" in names
        assert "Gamma Systems" in names
        assert "Beta Industries" not in names  # erp_system is None

    def test_state_filter(self, export_agent, sample_companies):
        result = export_agent._apply_filters(
            sample_companies, {"states": ["MI", "OH"], "min_quality": 0}
        )
        assert len(result) == 2
        states = {r["state"] for r in result}
        assert states == {"MI", "OH"}

    def test_industry_filter(self, export_agent, sample_companies):
        result = export_agent._apply_filters(
            sample_companies, {"industries": ["Welding"], "min_quality": 0}
        )
        assert len(result) == 1
        assert result[0]["company_name"] == "Beta Industries"

    def test_combined_filters(self, export_agent, sample_companies):
        result = export_agent._apply_filters(
            sample_companies,
            {
                "min_quality": 60,
                "has_erp": True,
                "states": ["MI"],
            },
        )
        # Only Acme passes all three
        assert len(result) == 1
        assert result[0]["company_name"] == "Acme Manufacturing"


# =========================================================================
# 3. CSV Export Tests
# =========================================================================

class TestExportCSV:
    """Tests for _export_csv."""

    def test_basic_csv_with_headers(self, export_agent, tmp_path, sample_companies):
        path = str(tmp_path / "out.csv")
        export_agent._export_csv(sample_companies, path, "companies")

        with open(path, encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
        assert "company_name" in header
        assert "website" in header
        assert "quality_score" in header

    def test_csv_row_count(self, export_agent, tmp_path, sample_companies):
        path = str(tmp_path / "out.csv")
        export_agent._export_csv(sample_companies, path, "companies")

        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        assert len(rows) == len(sample_companies)

    def test_csv_flattens_contacts(self, export_agent, tmp_path):
        records = [
            {
                "company_name": "Test Co",
                "contacts": [
                    {"full_name": "Alice", "email": "alice@test.com", "title": "CEO"}
                ],
            }
        ]
        path = str(tmp_path / "contacts.csv")
        export_agent._export_csv(records, path, "companies")

        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            row = next(reader)
        assert row["primary_contact_name"] == "Alice"
        assert row["primary_contact_email"] == "alice@test.com"
        assert row["contact_count"] == "1"

    def test_csv_empty_records_no_file(self, export_agent, tmp_path):
        path = str(tmp_path / "empty.csv")
        export_agent._export_csv([], path, "companies")
        assert not Path(path).exists()

    def test_csv_utf8_encoding(self, export_agent, tmp_path):
        records = [{"company_name": "Muller GmbH", "city": "Munchen"}]
        path = str(tmp_path / "utf8.csv")
        export_agent._export_csv(records, path, "companies")

        with open(path, encoding="utf-8") as f:
            content = f.read()
        assert "Muller GmbH" in content


# =========================================================================
# 4. JSON Export Tests
# =========================================================================

class TestExportJSON:
    """Tests for _export_json."""

    def test_basic_json_structure(self, export_agent, tmp_path, sample_companies):
        path = str(tmp_path / "out.json")
        export_agent._export_json(sample_companies, path, "companies")

        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        assert data["export_type"] == "companies"
        assert data["record_count"] == len(sample_companies)
        assert len(data["records"]) == len(sample_companies)

    def test_json_provenance_metadata(self, export_agent, tmp_path, sample_companies):
        path = str(tmp_path / "prov.json")
        export_agent._export_json(sample_companies, path, "companies")

        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        assert data["provenance"]["pipeline"] == "NAM Intelligence Pipeline"
        assert data["provenance"]["job_id"] == "test-job-123"
        assert data["provenance"]["agent"] == "export.export_activation"

    def test_json_exported_at_iso_format(self, export_agent, tmp_path):
        path = str(tmp_path / "ts.json")
        export_agent._export_json([{"company_name": "X"}], path, "companies")

        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        # Should be a valid ISO timestamp
        datetime.fromisoformat(data["exported_at"])

    def test_json_datetime_serialization(self, export_agent, tmp_path):
        records = [{"company_name": "X", "extracted_at": datetime.now(UTC)}]
        path = str(tmp_path / "dt.json")
        export_agent._export_json(records, path, "companies")

        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        # json.dump with default=str should convert datetime to string
        assert isinstance(data["records"][0]["extracted_at"], str)


# =========================================================================
# 5. CRM Export Tests
# =========================================================================

class TestExportCRM:
    """Tests for _export_crm (Salesforce / HubSpot mappings)."""

    def test_salesforce_field_mapping(self, export_agent, tmp_path):
        from agents.export.export_activation import ExportActivationAgent

        records = [
            {
                "company_name": "Acme",
                "website": "https://acme.com",
                "city": "Detroit",
                "state": "MI",
                "quality_score": 85,
            }
        ]
        path = str(tmp_path / "sf.csv")
        export_agent._export_crm(records, path, ExportActivationAgent.SALESFORCE_MAPPING)

        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            row = next(reader)

        assert row["Account Name"] == "Acme"
        assert row["Website"] == "https://acme.com"
        assert row["BillingCity"] == "Detroit"
        assert row["BillingState"] == "MI"
        assert row["Data_Quality_Score__c"] == "85"

    def test_hubspot_field_mapping(self, export_agent, tmp_path):
        from agents.export.export_activation import ExportActivationAgent

        records = [
            {
                "company_name": "Beta",
                "domain": "beta.com",
                "industry": "Welding",
            }
        ]
        path = str(tmp_path / "hs.csv")
        export_agent._export_crm(records, path, ExportActivationAgent.HUBSPOT_MAPPING)

        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            row = next(reader)

        assert row["name"] == "Beta"
        assert row["domain"] == "beta.com"
        assert row["industry"] == "Welding"

    def test_crm_list_values_joined(self, export_agent, tmp_path):
        from agents.export.export_activation import ExportActivationAgent

        records = [
            {
                "company_name": "Multi",
                "associations": ["PMA", "NEMA", "AGMA"],
                "quality_score": 80,
            }
        ]
        path = str(tmp_path / "crm_list.csv")
        export_agent._export_crm(records, path, ExportActivationAgent.SALESFORCE_MAPPING)

        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            row = next(reader)

        assert row["Associations__c"] == "PMA; NEMA; AGMA"

    def test_crm_data_source_field(self, export_agent, tmp_path):
        from agents.export.export_activation import ExportActivationAgent

        records = [{"company_name": "X"}]
        path = str(tmp_path / "src.csv")
        export_agent._export_crm(records, path, ExportActivationAgent.SALESFORCE_MAPPING)

        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            row = next(reader)

        assert row["Data_Source__c"] == "NAM Intelligence Pipeline"

    def test_crm_import_date_field(self, export_agent, tmp_path):
        from agents.export.export_activation import ExportActivationAgent

        records = [{"company_name": "Y"}]
        path = str(tmp_path / "date.csv")
        export_agent._export_crm(records, path, ExportActivationAgent.SALESFORCE_MAPPING)

        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            row = next(reader)

        # Should be today in YYYY-MM-DD format
        import_date = row["Import_Date__c"]
        datetime.strptime(import_date, "%Y-%m-%d")  # will raise if bad format

    def test_crm_empty_records_no_file(self, export_agent, tmp_path):
        from agents.export.export_activation import ExportActivationAgent

        path = str(tmp_path / "empty_crm.csv")
        export_agent._export_crm([], path, ExportActivationAgent.SALESFORCE_MAPPING)
        assert not Path(path).exists()


# =========================================================================
# 6. run() Method Tests
# =========================================================================

class TestRun:
    """Tests for the async run() entry point."""

    @pytest.mark.asyncio
    async def test_run_csv_success(self, export_agent, tmp_path, sample_companies):
        out = str(tmp_path / "run.csv")
        result = await export_agent.run({
            "export_type": "companies",
            "format": "csv",
            "records": sample_companies,
            "output_path": out,
        })
        assert result["success"] is True
        assert result["format"] == "csv"
        assert result["records_exported"] == len(sample_companies)
        assert Path(out).exists()

    @pytest.mark.asyncio
    async def test_run_json_success(self, export_agent, tmp_path, sample_companies):
        out = str(tmp_path / "run.json")
        result = await export_agent.run({
            "export_type": "companies",
            "format": "json",
            "records": sample_companies,
            "output_path": out,
        })
        assert result["success"] is True
        assert result["format"] == "json"
        assert Path(out).exists()

    @pytest.mark.asyncio
    async def test_run_salesforce_success(self, export_agent, tmp_path, sample_companies):
        out = str(tmp_path / "sf.csv")
        result = await export_agent.run({
            "export_type": "companies",
            "format": "salesforce",
            "records": sample_companies,
            "output_path": out,
        })
        assert result["success"] is True
        assert result["format"] == "salesforce"

    @pytest.mark.asyncio
    async def test_run_hubspot_success(self, export_agent, tmp_path, sample_companies):
        out = str(tmp_path / "hs.csv")
        result = await export_agent.run({
            "export_type": "companies",
            "format": "hubspot",
            "records": sample_companies,
            "output_path": out,
        })
        assert result["success"] is True
        assert result["format"] == "hubspot"

    @pytest.mark.asyncio
    async def test_run_unknown_format_error(self, export_agent, sample_companies):
        result = await export_agent.run({
            "format": "xml",
            "records": sample_companies,
        })
        assert result["success"] is False
        assert "Unknown format" in result["error"]

    @pytest.mark.asyncio
    async def test_run_empty_records_error(self, export_agent):
        result = await export_agent.run({
            "format": "csv",
            "records": [],
        })
        assert result["success"] is False
        assert "No records provided" in result["error"]

    @pytest.mark.asyncio
    async def test_run_applies_filters(self, export_agent, tmp_path, sample_companies):
        out = str(tmp_path / "filtered.csv")
        result = await export_agent.run({
            "format": "csv",
            "records": sample_companies,
            "filters": {"min_quality": 80},
            "output_path": out,
        })
        assert result["success"] is True
        # Only Acme (90) passes min_quality=80
        assert result["records_exported"] == 1
        assert result["records_processed"] == 3

    @pytest.mark.asyncio
    async def test_run_default_output_path(self, export_agent, sample_companies):
        """When output_path is not specified, agent auto-generates one."""
        result = await export_agent.run({
            "format": "csv",
            "records": sample_companies,
        })
        assert result["success"] is True
        assert "export_path" in result
        assert Path(result["export_path"]).exists()


# =========================================================================
# 7. Record Flattening Tests
# =========================================================================

class TestFlattenRecord:
    """Tests for _flatten_record."""

    def test_contacts_extraction(self, export_agent):
        record = {
            "company_name": "A",
            "contacts": [
                {"full_name": "Alice", "email": "a@a.com", "title": "CEO"},
                {"full_name": "Bob", "email": "b@a.com", "title": "CTO"},
            ],
        }
        flat = export_agent._flatten_record(record)
        assert flat["contact_count"] == 2
        assert flat["primary_contact_name"] == "Alice"
        assert flat["primary_contact_email"] == "a@a.com"
        assert flat["primary_contact_title"] == "CEO"

    def test_list_joining_associations(self, export_agent):
        record = {"associations": ["PMA", "NEMA"]}
        flat = export_agent._flatten_record(record)
        assert flat["associations"] == "PMA; NEMA"

    def test_list_joining_tech_stack(self, export_agent):
        record = {"tech_stack": ["SAP", "Salesforce", "AWS"]}
        flat = export_agent._flatten_record(record)
        assert flat["tech_stack"] == "SAP; Salesforce; AWS"

    def test_dict_flattening(self, export_agent):
        record = {"location": {"lat": 42.33, "lng": -83.05}}
        flat = export_agent._flatten_record(record)
        assert flat["location_lat"] == 42.33
        assert flat["location_lng"] == -83.05

    def test_datetime_handling(self, export_agent):
        now = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
        record = {"extracted_at": now}
        flat = export_agent._flatten_record(record)
        assert flat["extracted_at"] == now.isoformat()

    def test_provenance_and_meta_skipped(self, export_agent):
        record = {
            "company_name": "X",
            "provenance": [{"source": "pma.org"}],
            "_meta": {"job_id": "abc"},
        }
        flat = export_agent._flatten_record(record)
        assert "provenance" not in flat
        assert "_meta" not in flat
        assert flat["company_name"] == "X"

    def test_other_lists_json_dumped(self, export_agent):
        record = {"custom_tags": ["alpha", "beta"]}
        flat = export_agent._flatten_record(record)
        assert flat["custom_tags"] == json.dumps(["alpha", "beta"])

    def test_empty_contacts_not_added(self, export_agent):
        record = {"contacts": []}
        flat = export_agent._flatten_record(record)
        # contacts is a list, but empty -- goes through json.dumps path
        assert "contact_count" not in flat


# =========================================================================
# 8. Column Selection Tests
# =========================================================================

class TestGetColumns:
    """Tests for _get_columns."""

    def test_companies_columns(self, export_agent):
        cols = export_agent._get_columns("companies", [])
        assert "company_name" in cols
        assert "quality_score" in cols
        assert "primary_contact_name" in cols
        assert "erp_system" in cols

    def test_events_columns(self, export_agent):
        cols = export_agent._get_columns("events", [])
        assert "title" in cols
        assert "event_type" in cols
        assert "is_virtual" in cols
        assert "registration_url" in cols

    def test_participants_columns(self, export_agent):
        cols = export_agent._get_columns("participants", [])
        assert "company_name" in cols
        assert "sponsor_tier" in cols
        assert "booth_number" in cols
        assert "speaker_name" in cols

    def test_competitors_columns(self, export_agent):
        cols = export_agent._get_columns("competitors", [])
        assert "competitor_name" in cols
        assert "signal_type" in cols
        assert "confidence" in cols

    def test_auto_detect_from_records(self, export_agent):
        records = [{"custom_a": 1, "custom_b": 2}]
        cols = export_agent._get_columns("unknown_type", records)
        assert cols == ["custom_a", "custom_b"]

    def test_auto_detect_empty_records(self, export_agent):
        cols = export_agent._get_columns("unknown_type", [])
        assert cols == []


# =========================================================================
# 9. generate_competitor_report Tests
# =========================================================================

class TestGenerateCompetitorReport:
    """Tests for generate_competitor_report."""

    @pytest.mark.asyncio
    async def test_success(self, export_agent, tmp_path, sample_signals):
        out = str(tmp_path / "comp_report.json")
        with patch(
            "agents.intelligence.competitor_signal_miner.CompetitorReportGenerator"
        ) as MockGen:
            MockGen.generate_report.return_value = {
                "total_signals": 3,
                "competitors": {
                    "sap": {"name": "SAP", "total_signals": 2},
                    "oracle": {"name": "Oracle", "total_signals": 1},
                },
            }

            result = await export_agent.generate_competitor_report({
                "signals": sample_signals,
                "output_path": out,
            })

        assert result["success"] is True
        assert result["competitors_analyzed"] == 2
        assert result["total_signals"] == 3
        assert Path(out).exists()

        with open(out, encoding="utf-8") as f:
            data = json.load(f)
        assert data["report_type"] == "Competitor Intelligence"
        assert data["pipeline"] == "NAM Intelligence Pipeline"
        assert data["job_id"] == "test-job-123"

    @pytest.mark.asyncio
    async def test_empty_signals_error(self, export_agent):
        result = await export_agent.generate_competitor_report({"signals": []})
        assert result["success"] is False
        assert "No signals" in result["error"]

    @pytest.mark.asyncio
    async def test_default_output_path(self, export_agent, sample_signals):
        with patch(
            "agents.intelligence.competitor_signal_miner.CompetitorReportGenerator"
        ) as MockGen:
            MockGen.generate_report.return_value = {
                "total_signals": 3,
                "competitors": {"sap": {"total_signals": 2}},
            }
            result = await export_agent.generate_competitor_report({
                "signals": sample_signals,
            })

        assert result["success"] is True
        assert Path(result["export_path"]).exists()


# =========================================================================
# 10. generate_summary_report Tests
# =========================================================================

class TestGenerateSummaryReport:
    """Tests for generate_summary_report."""

    @pytest.mark.asyncio
    async def test_success_with_all_data(
        self, export_agent, tmp_path, sample_companies, sample_events, sample_signals
    ):
        out = str(tmp_path / "summary.json")
        result = await export_agent.generate_summary_report({
            "companies": sample_companies,
            "events": sample_events,
            "signals": sample_signals,
            "output_path": out,
        })
        assert result["success"] is True
        assert result["records_processed"] == (
            len(sample_companies) + len(sample_events) + len(sample_signals)
        )

        summary = result["summary"]
        assert summary["totals"]["companies"] == 3
        assert summary["totals"]["events"] == 2
        assert summary["totals"]["competitor_signals"] == 3
        assert summary["report_type"] == "Pipeline Summary"
        assert summary["pipeline"] == "NAM Intelligence Pipeline"
        assert summary["job_id"] == "test-job-123"

    @pytest.mark.asyncio
    async def test_empty_data(self, export_agent, tmp_path):
        out = str(tmp_path / "empty_summary.json")
        result = await export_agent.generate_summary_report({"output_path": out})
        assert result["success"] is True
        summary = result["summary"]
        assert summary["totals"]["companies"] == 0
        assert summary["company_stats"] == {}
        assert summary["event_stats"] == {}
        assert summary["signal_stats"] == {}

    @pytest.mark.asyncio
    async def test_output_file_written(self, export_agent, tmp_path, sample_companies):
        out = str(tmp_path / "summary_file.json")
        await export_agent.generate_summary_report({
            "companies": sample_companies,
            "output_path": out,
        })
        with open(out, encoding="utf-8") as f:
            data = json.load(f)
        assert data["report_type"] == "Pipeline Summary"
        assert data["totals"]["companies"] == 3


# =========================================================================
# 11. Stats Computation Tests
# =========================================================================

class TestComputeCompanyStats:
    """Tests for _compute_company_stats."""

    def test_quality_distribution(self, export_agent, sample_companies):
        stats = export_agent._compute_company_stats(sample_companies)
        assert stats["quality_distribution"]["A"] == 1
        assert stats["quality_distribution"]["B"] == 1
        assert stats["quality_distribution"]["D"] == 1

    def test_association_distribution(self, export_agent, sample_companies):
        stats = export_agent._compute_company_stats(sample_companies)
        assert stats["association_distribution"]["PMA"] == 2
        assert stats["association_distribution"]["NEMA"] == 2

    def test_state_distribution(self, export_agent, sample_companies):
        stats = export_agent._compute_company_stats(sample_companies)
        assert stats["state_distribution"]["MI"] == 1
        assert stats["state_distribution"]["IL"] == 1
        assert stats["state_distribution"]["OH"] == 1

    def test_erp_distribution(self, export_agent, sample_companies):
        stats = export_agent._compute_company_stats(sample_companies)
        assert "SAP" in stats["erp_distribution"]
        assert "Oracle" in stats["erp_distribution"]

    def test_avg_quality_score(self, export_agent, sample_companies):
        stats = export_agent._compute_company_stats(sample_companies)
        expected_avg = (90 + 70 + 45) / 3
        assert abs(stats["avg_quality_score"] - expected_avg) < 0.01

    def test_with_contacts_count(self, export_agent, sample_companies):
        stats = export_agent._compute_company_stats(sample_companies)
        assert stats["with_contacts"] == 2  # Acme and Gamma

    def test_with_erp_count(self, export_agent, sample_companies):
        stats = export_agent._compute_company_stats(sample_companies)
        assert stats["with_erp"] == 2  # Acme (SAP) and Gamma (Oracle)

    def test_empty_input(self, export_agent):
        assert export_agent._compute_company_stats([]) == {}


class TestComputeEventStats:
    """Tests for _compute_event_stats."""

    def test_type_distribution(self, export_agent, sample_events):
        stats = export_agent._compute_event_stats(sample_events)
        assert stats["type_distribution"]["TRADE_SHOW"] == 1
        assert stats["type_distribution"]["WEBINAR"] == 1

    def test_virtual_count(self, export_agent, sample_events):
        stats = export_agent._compute_event_stats(sample_events)
        assert stats["virtual_count"] == 1

    def test_with_registration_url(self, export_agent, sample_events):
        stats = export_agent._compute_event_stats(sample_events)
        assert stats["with_registration_url"] == 1

    def test_empty_input(self, export_agent):
        assert export_agent._compute_event_stats([]) == {}


class TestComputeSignalStats:
    """Tests for _compute_signal_stats."""

    def test_competitor_distribution(self, export_agent, sample_signals):
        stats = export_agent._compute_signal_stats(sample_signals)
        assert stats["competitor_distribution"]["SAP"] == 2
        assert stats["competitor_distribution"]["Oracle"] == 1

    def test_signal_type_distribution(self, export_agent, sample_signals):
        stats = export_agent._compute_signal_stats(sample_signals)
        assert stats["signal_type_distribution"]["SPONSORSHIP"] == 2
        assert stats["signal_type_distribution"]["EXHIBITOR"] == 1

    def test_empty_input(self, export_agent):
        assert export_agent._compute_signal_stats([]) == {}
