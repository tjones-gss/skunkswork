"""
Pytest Fixtures for NAM Intelligence Pipeline Tests

Shared fixtures for contract validation and state machine testing.
"""

import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

# =============================================================================
# PATH FIXTURES
# =============================================================================


@pytest.fixture
def project_root() -> Path:
    """Get project root directory."""
    return Path(__file__).parent.parent


@pytest.fixture
def contracts_dir(project_root) -> Path:
    """Get contracts/schemas directory."""
    return project_root / "contracts" / "schemas"


@pytest.fixture
def fixtures_dir() -> Path:
    """Get test fixtures directory."""
    return Path(__file__).parent / "fixtures"


# =============================================================================
# VALIDATOR FIXTURES
# =============================================================================


@pytest.fixture
def validator(contracts_dir):
    """Create a ContractValidator instance."""
    from contracts.validator import ContractValidator
    return ContractValidator(contracts_dir)


@pytest.fixture
def global_validator():
    """Get the global validator instance."""
    # Reset global validator for clean tests
    import contracts.validator as validator_module
    from contracts.validator import get_validator
    validator_module._validator = None
    return get_validator()


# =============================================================================
# VALID DATA FIXTURES
# =============================================================================


@pytest.fixture
def valid_company() -> dict:
    """Valid company entity."""
    return {
        "company_name": "Acme Manufacturing Inc.",
        "domain": "acme-mfg.com",
        "website": "https://www.acme-mfg.com",
        "city": "Detroit",
        "state": "MI",
        "country": "United States",
        "employee_count_min": 100,
        "employee_count_max": 500,
        "revenue_min_usd": 10000000,
        "revenue_max_usd": 50000000,
        "naics_code": "332710",
        "industry": "Machine Shops",
        "erp_system": "SAP",
        "crm_system": "Salesforce",
        "tech_stack": ["AWS", "PostgreSQL", "React"],
        "associations": ["PMA", "NEMA"],
        "contacts": [],
        "quality_score": 85,
        "quality_grade": "A",
        "data_sources": ["PMA_directory"],
        "provenance": [],
        "created_at": "2024-01-15T10:30:00Z",
        "updated_at": "2024-01-15T10:30:00Z"
    }


@pytest.fixture
def valid_contact() -> dict:
    """Valid contact entity."""
    return {
        "full_name": "John Smith",
        "first_name": "John",
        "last_name": "Smith",
        "title": "VP of Operations",
        "department": "Operations",
        "seniority": "VP",
        "email": "jsmith@acme-mfg.com",
        "email_verified": True,
        "phone": "+1-555-123-4567",
        "linkedin_url": "https://www.linkedin.com/in/johnsmith",
        "data_source": "apollo",
        "confidence_score": 0.95
    }


@pytest.fixture
def valid_event() -> dict:
    """Valid event entity."""
    return {
        "title": "FABTECH 2024",
        "event_type": "TRADE_SHOW",
        "description": "North America's largest metal forming, fabricating, welding and finishing event",
        "start_date": "2024-10-15T09:00:00Z",
        "end_date": "2024-10-17T17:00:00Z",
        "venue": "Orlando Convention Center",
        "city": "Orlando",
        "state": "FL",
        "country": "United States",
        "is_virtual": False,
        "event_url": "https://www.fabtechexpo.com",
        "organizer_name": "SME",
        "organizer_association": "SME",
        "expected_attendees": 35000,
        "exhibitor_count": 1500,
        "provenance": [],
        "created_at": "2024-01-10T08:00:00Z",
        "updated_at": "2024-01-10T08:00:00Z"
    }


@pytest.fixture
def valid_provenance() -> dict:
    """Valid provenance record."""
    return {
        "source_url": "https://www.pma.org/members/acme-manufacturing",
        "source_type": "web",
        "extracted_at": "2024-01-15T10:30:00Z",
        "extracted_by": "extraction.html_parser",
        "association_code": "PMA",
        "job_id": str(uuid.uuid4()),
        "page_type": "MEMBER_DETAIL",
        "confidence": 0.95
    }


@pytest.fixture
def valid_access_verdict() -> dict:
    """Valid access verdict entity."""
    return {
        "url": "https://www.pma.org/members",
        "domain": "pma.org",
        "is_allowed": True,
        "reasons": ["robots.txt allows", "no auth required"],
        "robots_txt_exists": True,
        "robots_txt_allows": True,
        "crawl_delay": 2.0,
        "requires_auth": False,
        "auth_type": None,
        "suggested_rate": 0.5,
        "daily_limit": 1000,
        "tos_reviewed": True,
        "tos_allows_crawling": True,
        "checked_at": "2024-01-15T10:00:00Z"
    }


# =============================================================================
# INVALID DATA FIXTURES
# =============================================================================


@pytest.fixture
def invalid_company_missing_name() -> dict:
    """Company missing required company_name field."""
    return {
        "domain": "invalid.com",
        "city": "Chicago",
        "state": "IL"
    }


@pytest.fixture
def invalid_company_bad_score() -> dict:
    """Company with quality_score out of range."""
    return {
        "company_name": "Bad Score Inc",
        "quality_score": 150  # Max is 100
    }


@pytest.fixture
def invalid_company_bad_grade() -> dict:
    """Company with invalid quality_grade."""
    return {
        "company_name": "Bad Grade Inc",
        "quality_grade": "X"  # Valid: A, B, C, D, F
    }


@pytest.fixture
def invalid_contact_missing_name() -> dict:
    """Contact missing required full_name field."""
    return {
        "email": "nobody@example.com",
        "title": "Unknown"
    }


@pytest.fixture
def invalid_contact_bad_confidence() -> dict:
    """Contact with confidence_score out of range."""
    return {
        "full_name": "Test User",
        "confidence_score": 1.5  # Max is 1.0
    }


# =============================================================================
# AGENT I/O FIXTURES
# =============================================================================


@pytest.fixture
def valid_gatekeeper_input() -> dict:
    """Valid input for AccessGatekeeperAgent."""
    return {
        "urls": [
            "https://www.pma.org",
            "https://www.nema.org"
        ],
        "association_code": "PMA",
        "check_robots": True,
        "check_tos": False,
        "check_auth": True
    }


@pytest.fixture
def valid_gatekeeper_output(valid_access_verdict) -> dict:
    """Valid output from AccessGatekeeperAgent."""
    return {
        "success": True,
        "records_processed": 2,
        "verdicts": [valid_access_verdict],
        "allowed_urls": ["https://www.pma.org/members"],
        "blocked_urls": []
    }


@pytest.fixture
def valid_html_parser_input() -> dict:
    """Valid input for HTMLParserAgent."""
    return {
        "pages": [
            {
                "url": "https://www.pma.org/members",
                "content_path": "data/raw/PMA/pages/members.html",
                "page_type": "MEMBER_DIRECTORY"
            }
        ],
        "association_code": "PMA",
        "extract_links": True
    }


@pytest.fixture
def invalid_gatekeeper_input_empty_urls() -> dict:
    """Invalid gatekeeper input with empty urls array."""
    return {
        "urls": [],  # minItems: 1
        "check_robots": True
    }


@pytest.fixture
def invalid_gatekeeper_input_missing_urls() -> dict:
    """Invalid gatekeeper input missing required urls field."""
    return {
        "association_code": "PMA",
        "check_robots": True
    }


# =============================================================================
# STATE FIXTURES
# =============================================================================


@pytest.fixture
def fresh_pipeline_state():
    """Create a fresh PipelineState instance."""
    from state.machine import PipelineState
    return PipelineState(
        association_codes=["PMA", "NEMA"]
    )


@pytest.fixture
def state_manager(tmp_path):
    """Create a StateManager with temporary directory."""
    from state.machine import StateManager
    return StateManager(state_dir=str(tmp_path / "state"))


@pytest.fixture
def populated_pipeline_state():
    """PipelineState with some data populated."""
    from state.machine import PipelineState

    state = PipelineState(
        job_id="test-job-123",
        association_codes=["PMA"]
    )

    # Add some queue items
    state.add_to_queue("https://pma.org/members", priority=1, association="PMA")
    state.add_to_queue("https://pma.org/events", priority=2, association="PMA")

    # Mark some as visited
    state.mark_visited("https://pma.org")

    # Add a company
    state.add_company({
        "company_name": "Test Company",
        "domain": "test.com"
    })

    # Add an event
    state.add_event({
        "title": "Test Event",
        "event_type": "CONFERENCE"
    })

    return state


@pytest.fixture
def state_in_discovery(fresh_pipeline_state):
    """PipelineState that has transitioned to DISCOVERY phase."""
    from state.machine import PipelinePhase

    state = fresh_pipeline_state
    state.transition_to(PipelinePhase.GATEKEEPER)
    state.transition_to(PipelinePhase.DISCOVERY)
    return state


# =============================================================================
# MOCK FIXTURES
# =============================================================================


@pytest.fixture
def mock_agent_spawner():
    """Mock AgentSpawner for orchestrator tests."""
    spawner = MagicMock()
    spawner.job_id = str(uuid.uuid4())

    # Mock spawn method
    async def mock_spawn(agent_type, task, timeout=300):
        return {
            "success": True,
            "records_processed": 10,
            "data": [],
            "_meta": {
                "agent_type": agent_type,
                "job_id": spawner.job_id
            }
        }

    spawner.spawn = AsyncMock(side_effect=mock_spawn)

    # Mock spawn_parallel method
    async def mock_spawn_parallel(agent_type, tasks, max_concurrent=5, timeout=300):
        results = []
        for task in tasks:
            result = await mock_spawn(agent_type, task, timeout)
            results.append(result)
        return results

    spawner.spawn_parallel = AsyncMock(side_effect=mock_spawn_parallel)

    return spawner


@pytest.fixture
def mock_failing_spawner():
    """Mock AgentSpawner that simulates failures."""
    spawner = MagicMock()
    spawner.job_id = str(uuid.uuid4())

    async def mock_spawn(agent_type, task, timeout=300):
        return {
            "success": False,
            "error": "Simulated failure",
            "error_type": "TestError",
            "records_processed": 0
        }

    spawner.spawn = AsyncMock(side_effect=mock_spawn)

    return spawner


# =============================================================================
# HELPER FIXTURES
# =============================================================================


@pytest.fixture
def sample_urls() -> list[str]:
    """Sample URLs for testing."""
    return [
        "https://www.pma.org/members",
        "https://www.pma.org/events",
        "https://www.nema.org/directory",
        "https://www.socma.org/about"
    ]


@pytest.fixture
def sample_company_records() -> list[dict]:
    """Sample company records for batch testing."""
    return [
        {"company_name": "Company A", "domain": "company-a.com", "state": "MI"},
        {"company_name": "Company B", "domain": "company-b.com", "state": "OH"},
        {"company_name": "Company C", "domain": "company-c.com", "state": "IL"},
    ]


@pytest.fixture
def json_file(tmp_path):
    """Factory fixture to create temporary JSON files."""
    def _create_json(data: Any, filename: str = "test.json") -> Path:
        path = tmp_path / filename
        with open(path, "w") as f:
            json.dump(data, f)
        return path
    return _create_json


@pytest.fixture
def jsonl_file(tmp_path):
    """Factory fixture to create temporary JSONL files."""
    def _create_jsonl(records: list[dict], filename: str = "test.jsonl") -> Path:
        path = tmp_path / filename
        with open(path, "w", encoding="utf-8") as f:
            for record in records:
                f.write(json.dumps(record) + "\n")
        return path
    return _create_jsonl


# =============================================================================
# HTTP CLIENT MOCKS
# =============================================================================


@pytest.fixture
def mock_http_client():
    """Mock AsyncHTTPClient."""
    client = AsyncMock()
    client.get = AsyncMock()
    client.post = AsyncMock()
    client.close = AsyncMock()
    return client


@pytest.fixture
def mock_http_response():
    """Factory for mock HTTP responses."""
    def _create(status_code: int = 200, json_data: dict = None, text: str = "", headers: dict = None):
        response = MagicMock()
        response.status_code = status_code
        response.text = text
        response.headers = headers or {}
        response.json = MagicMock(return_value=json_data or {})
        return response
    return _create


# =============================================================================
# API RESPONSE FIXTURES
# =============================================================================


@pytest.fixture
def clearbit_response() -> dict:
    """Valid Clearbit company API response."""
    return {
        "name": "Acme Manufacturing",
        "domain": "acme-mfg.com",
        "description": "Leading manufacturer of precision parts",
        "foundedYear": 1985,
        "metrics": {
            "employees": 250,
            "employeesRange": "201-500",
            "estimatedAnnualRevenue": "$10M-$50M"
        },
        "category": {
            "naicsCode": "332710",
            "industry": "Manufacturing",
            "subIndustry": "Machine Shops"
        },
        "linkedin": {
            "handle": "acme-mfg"
        },
        "location": "Detroit, MI, USA"
    }


@pytest.fixture
def builtwith_response() -> dict:
    """Valid BuiltWith API response."""
    return {
        "Results": [{
            "Result": {
                "Paths": [{
                    "Technologies": [
                        {"Name": "SAP", "Categories": ["ERP Systems"]},
                        {"Name": "Salesforce", "Categories": ["CRM"]},
                        {"Name": "WordPress", "Categories": ["CMS"]},
                        {"Name": "Google Analytics", "Categories": ["Analytics"]},
                        {"Name": "AWS", "Categories": ["Hosting"]}
                    ]
                }]
            }
        }]
    }


@pytest.fixture
def apollo_organization_response() -> dict:
    """Valid Apollo organization enrichment response."""
    return {
        "organization": {
            "id": "5d1b01c1a1d1cc0001a32c1a",
            "name": "Acme Manufacturing",
            "website_url": "https://acme-mfg.com",
            "estimated_num_employees": 250,
            "founded_year": 1985,
            "industry": "Manufacturing",
            "linkedin_url": "https://linkedin.com/company/acme-mfg",
            "primary_domain": "acme-mfg.com"
        }
    }


@pytest.fixture
def zoominfo_response() -> dict:
    """Valid ZoomInfo company search response."""
    return {
        "data": [{
            "companyId": 123456,
            "companyName": "Acme Manufacturing",
            "domain": "acme-mfg.com",
            "employeeCount": 250,
            "revenueInMillions": 25,
            "yearFounded": 1985,
            "naicsCode": "332710",
            "city": "Detroit",
            "state": "MI"
        }]
    }


# =============================================================================
# HTML FIXTURES
# =============================================================================


@pytest.fixture
def sample_member_directory_html() -> str:
    """Sample member directory HTML for parser tests."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Member Directory</title></head>
    <body>
        <div class="directory">
            <div class="member-item">
                <h3 class="company-name">Acme Manufacturing Inc.</h3>
                <p class="location">Detroit, MI</p>
                <a class="website" href="https://acme-mfg.com">Website</a>
                <span class="phone">(555) 123-4567</span>
            </div>
            <div class="member-item">
                <h3 class="company-name">Beta Industries LLC</h3>
                <p class="location">Chicago, IL</p>
                <a class="website" href="https://beta-industries.com">Website</a>
                <span class="phone">(555) 987-6543</span>
            </div>
            <div class="member-item">
                <h3 class="company-name">Gamma Systems Corp</h3>
                <p class="location">Cleveland, OH</p>
                <a class="website" href="https://gamma-systems.com">Website</a>
                <span class="phone">(555) 456-7890</span>
            </div>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def sample_member_detail_html() -> str:
    """Sample member detail page HTML for parser tests."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Acme Manufacturing - PMA Member</title></head>
    <body>
        <div class="member-profile">
            <h1 class="company-name">Acme Manufacturing Inc.</h1>
            <div class="contact-info">
                <p class="address">123 Industrial Blvd, Detroit, MI 48201</p>
                <p class="phone">Phone: (555) 123-4567</p>
                <p class="fax">Fax: (555) 123-4568</p>
                <a class="website" href="https://acme-mfg.com">https://acme-mfg.com</a>
                <a class="email" href="mailto:info@acme-mfg.com">info@acme-mfg.com</a>
            </div>
            <div class="company-info">
                <p class="description">Acme Manufacturing is a leading provider of precision machined parts.</p>
                <p class="employees">Employees: 250-500</p>
                <p class="year-founded">Founded: 1985</p>
            </div>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def sample_schema() -> dict:
    """Sample extraction schema for parser tests."""
    return {
        "company_name": {
            "selectors": ["h1.company-name", ".company-name", "h1"],
            "parser": "title_case"
        },
        "city": {
            "selectors": [".address", ".location"],
            "parser": "title_case"
        },
        "state": {
            "selectors": [".address", ".location"],
            "parser": "state_code"
        },
        "phone": {
            "selectors": [".phone", ".contact-phone"],
            "parser": "phone"
        },
        "website": {
            "selectors": ["a.website"],
            "extract": "href"
        },
        "list_container": ".directory",
        "list_item": ".member-item"
    }


# =============================================================================
# MOCK API KEYS
# =============================================================================


@pytest.fixture
def mock_api_keys(monkeypatch):
    """Set mock API keys for testing."""
    monkeypatch.setenv("CLEARBIT_API_KEY", "test-clearbit-key")
    monkeypatch.setenv("BUILTWITH_API_KEY", "test-builtwith-key")
    monkeypatch.setenv("APOLLO_API_KEY", "test-apollo-key")
    monkeypatch.setenv("ZOOMINFO_API_KEY", "test-zoominfo-key")
    monkeypatch.setenv("HUNTER_API_KEY", "test-hunter-key")


@pytest.fixture
def no_api_keys(monkeypatch):
    """Ensure no API keys are set."""
    monkeypatch.delenv("CLEARBIT_API_KEY", raising=False)
    monkeypatch.delenv("BUILTWITH_API_KEY", raising=False)
    monkeypatch.delenv("APOLLO_API_KEY", raising=False)
    monkeypatch.delenv("ZOOMINFO_API_KEY", raising=False)
    monkeypatch.delenv("HUNTER_API_KEY", raising=False)


# =============================================================================
# MOCK AGENT COMPONENTS
# =============================================================================


@pytest.fixture
def mock_config():
    """Mock Config class."""
    config = MagicMock()
    config.load = MagicMock(return_value={})
    config.get = MagicMock(return_value=None)
    return config


@pytest.fixture
def mock_logger():
    """Mock StructuredLogger."""
    logger = MagicMock()
    logger.debug = MagicMock()
    logger.info = MagicMock()
    logger.warning = MagicMock()
    logger.error = MagicMock()
    return logger


@pytest.fixture
def mock_rate_limiter():
    """Mock RateLimiter."""
    limiter = MagicMock()
    limiter.acquire = AsyncMock()
    limiter.get_rate = MagicMock(return_value=1.0)
    return limiter


@pytest.fixture
def mock_jsonl_writer():
    """Mock JSONLWriter context manager."""
    writer = MagicMock()
    writer.__enter__ = MagicMock(return_value=writer)
    writer.__exit__ = MagicMock(return_value=False)
    writer.write = MagicMock()
    writer.write_batch = MagicMock()
    return writer


# =============================================================================
# VALIDATION AGENT FIXTURES
# =============================================================================


@pytest.fixture
def duplicate_company_records():
    """Company records with duplicates for dedupe tests."""
    return [
        {
            "company_name": "Acme Manufacturing Inc",
            "domain": "acme-mfg.com",
            "website": "https://acme-mfg.com",
            "city": "Detroit",
            "state": "MI",
            "employee_count_min": 100,
        },
        {
            "company_name": "ACME Manufacturing",
            "domain": "acme-mfg.com",
            "website": "https://www.acme-mfg.com",
            "city": "Detroit",
            "state": "MI",
            "employee_count_min": 150,
        },
        {
            "company_name": "Beta Industries",
            "domain": "beta-ind.com",
            "website": "https://beta-ind.com",
            "city": "Chicago",
            "state": "IL",
        },
    ]


@pytest.fixture
def scored_company_record():
    """Company record with validation results for scorer tests."""
    from datetime import UTC
    return {
        "company_name": "Test Corp",
        "website": "https://test.com",
        "city": "Detroit",
        "state": "MI",
        "employee_count_min": 100,
        "erp_system": "SAP",
        "contacts": [{"name": "John", "title": "CEO"}],
        "extracted_at": datetime.now(UTC).isoformat(),
        "firmographic_source": "clearbit",
        "tech_source": "builtwith",
        "association": "PMA",
        "_validation": {
            "dns_mx_valid": True,
            "google_places_matched": True
        }
    }


@pytest.fixture
def mock_dns_resolver():
    """Mock dns.resolver for CrossRef tests."""
    resolver = MagicMock()
    resolver.resolve = MagicMock()
    resolver.NXDOMAIN = type("NXDOMAIN", (Exception,), {})
    resolver.NoAnswer = type("NoAnswer", (Exception,), {})
    return resolver


@pytest.fixture
def google_places_api_response():
    """Mock Google Places API response."""
    return {
        "results": [
            {
                "name": "Acme Manufacturing Inc",
                "formatted_address": "123 Industrial Blvd, Detroit, MI 48201",
                "place_id": "ChIJ1234567890"
            }
        ],
        "status": "OK"
    }


@pytest.fixture
def entity_resolver_records():
    """Records for entity resolution testing."""
    return [
        {
            "company_name": "Acme Manufacturing Inc.",
            "website": "https://acme-mfg.com",
            "city": "Detroit",
            "state": "MI",
            "quality_score": 85,
            "phone": "(555) 123-4567",
            "provenance": [{"source_url": "https://pma.org/members/acme"}],
            "association": "PMA"
        },
        {
            "company_name": "ACME Mfg",
            "website": "https://www.acme-mfg.com",
            "city": "Detroit",
            "state": "MI",
            "quality_score": 75,
            "phone": "555-123-4567",
            "provenance": [{"source_url": "https://nema.org/directory/acme"}],
            "association": "NEMA"
        },
        {
            "company_name": "Beta Industries LLC",
            "website": "https://beta-ind.com",
            "city": "Chicago",
            "state": "IL",
            "quality_score": 80,
        },
    ]


# =============================================================================
# ENRICHMENT AGENT FIXTURES
# =============================================================================


@pytest.fixture
def apollo_contacts_response():
    """Mock Apollo people search response."""
    return {
        "people": [
            {
                "name": "John Smith",
                "first_name": "John",
                "last_name": "Smith",
                "title": "Chief Information Officer",
                "email": "jsmith@acme.com",
                "phone_numbers": [{"number": "555-123-4567", "sanitized_number": "5551234567"}],
                "linkedin_url": "https://linkedin.com/in/johnsmith"
            },
            {
                "name": "Jane Doe",
                "first_name": "Jane",
                "last_name": "Doe",
                "title": "CFO",
                "email": "jdoe@acme.com",
                "phone_numbers": [],
                "linkedin_url": "https://linkedin.com/in/janedoe"
            }
        ]
    }


@pytest.fixture
def zoominfo_contacts_response():
    """Mock ZoomInfo contact search response."""
    return {
        "data": [
            {
                "firstName": "John",
                "lastName": "Smith",
                "jobTitle": "CIO",
                "email": "jsmith@acme.com",
                "phone": "555-123-4567",
                "linkedInUrl": "https://linkedin.com/in/johnsmith"
            }
        ]
    }


@pytest.fixture
def team_page_html():
    """Mock company team/leadership page HTML."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Leadership Team</title></head>
    <body>
        <div class="leadership">
            <div class="team-member">
                <h3>John Smith</h3>
                <p class="position">Chief Information Officer</p>
                <a href="mailto:jsmith@acme.com">jsmith@acme.com</a>
                <a href="tel:+15551234567">555-123-4567</a>
                <a href="https://linkedin.com/in/johnsmith">LinkedIn</a>
            </div>
            <div class="team-member">
                <h3>Jane Doe</h3>
                <p class="position">Chief Financial Officer</p>
                <a href="mailto:jdoe@acme.com">jdoe@acme.com</a>
            </div>
            <div class="team-member">
                <h3>Bob Johnson</h3>
                <p class="position">VP of Operations</p>
            </div>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def about_page_html():
    """Mock company about page HTML for firmographic scraping."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>About Acme Manufacturing</title></head>
    <body>
        <div class="about-content">
            <h1>About Acme Manufacturing</h1>
            <p>Founded in 1985, Acme Manufacturing has grown to become a
            leading provider of precision machined parts.</p>
            <p>With over 250 employees and annual revenue of $50 million,
            we serve customers across North America.</p>
            <p>Our team of dedicated professionals works around the clock
            to deliver quality products on time.</p>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def website_with_tech_fingerprint():
    """Mock company website HTML with tech stack fingerprints."""
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Acme Manufacturing</title>
        <script src="/sap-ui-core.js"></script>
        <script src="https://js.hs-scripts.com/1234567.js"></script>
        <link href="/wp-content/themes/theme/style.css">
    </head>
    <body>
        <script src="https://www.google-analytics.com/analytics.js"></script>
        <div id="react-root"></div>
    </body>
    </html>
    """


@pytest.fixture
def job_posting_page_html():
    """Mock job posting page HTML with ERP mentions."""
    return """
    <!DOCTYPE html>
    <html>
    <head><title>Jobs at Acme Manufacturing</title></head>
    <body>
        <div class="job-listings">
            <div class="job">
                <h3>ERP Administrator</h3>
                <p>We are looking for an experienced SAP administrator to manage
                our SAP S/4HANA implementation. SAP certification required.</p>
            </div>
            <div class="job">
                <h3>IT Manager</h3>
                <p>Oversee our IT infrastructure including SAP Business One
                and Salesforce CRM systems.</p>
            </div>
        </div>
    </body>
    </html>
    """


@pytest.fixture
def enrichment_records():
    """Sample records for enrichment testing."""
    return [
        {
            "company_name": "Acme Manufacturing Inc",
            "website": "https://acme-mfg.com",
            "domain": "acme-mfg.com",
            "city": "Detroit",
            "state": "MI"
        },
        {
            "company_name": "Beta Industries LLC",
            "website": "https://beta-ind.com",
            "domain": "beta-ind.com",
            "city": "Chicago",
            "state": "IL"
        },
        {
            "company_name": "Gamma Systems Corp",
            "website": "https://gamma-systems.com",
            "domain": "gamma-systems.com",
            "city": "Cleveland",
            "state": "OH"
        }
    ]


@pytest.fixture
def enriched_record():
    """Sample fully enriched company record."""
    return {
        "company_name": "Acme Manufacturing Inc",
        "website": "https://acme-mfg.com",
        "domain": "acme-mfg.com",
        "city": "Detroit",
        "state": "MI",
        "employee_count_min": 250,
        "employee_count_max": 250,
        "revenue_min_usd": 50000000,
        "year_founded": 1985,
        "naics_code": "332710",
        "industry": "Manufacturing",
        "linkedin_url": "https://linkedin.com/company/acme-mfg",
        "firmographic_source": "clearbit",
        "erp_system": "SAP",
        "crm_system": "Salesforce",
        "tech_stack": ["SAP", "Salesforce", "WordPress", "Google Analytics"],
        "tech_source": "builtwith",
        "contacts": [
            {
                "name": "John Smith",
                "title": "CIO",
                "email": "jsmith@acme.com",
                "phone": "555-123-4567",
                "source": "apollo"
            }
        ]
    }


# =============================================================================
# DISCOVERY AGENT FIXTURES
# =============================================================================


@pytest.fixture
def robots_txt_allow_all() -> str:
    """robots.txt that allows all crawling."""
    return """
User-agent: *
Allow: /
Sitemap: https://example.com/sitemap.xml
"""


@pytest.fixture
def robots_txt_block_directory() -> str:
    """robots.txt that blocks member directory."""
    return """
User-agent: *
Allow: /
Disallow: /members
Disallow: /directory
"""


@pytest.fixture
def robots_txt_with_crawl_delay() -> str:
    """robots.txt with crawl delay specified."""
    return """
User-agent: *
Allow: /
Crawl-delay: 5
"""


@pytest.fixture
def sitemap_xml_with_directory() -> str:
    """sitemap.xml containing member directory URL."""
    return """<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
    <url>
        <loc>https://example.com/</loc>
        <lastmod>2024-01-01</lastmod>
    </url>
    <url>
        <loc>https://example.com/about</loc>
        <lastmod>2024-01-01</lastmod>
    </url>
    <url>
        <loc>https://example.com/member-directory</loc>
        <lastmod>2024-01-15</lastmod>
    </url>
    <url>
        <loc>https://example.com/events</loc>
        <lastmod>2024-01-10</lastmod>
    </url>
</urlset>
"""


@pytest.fixture
def member_directory_with_pagination_html() -> str:
    """Member directory HTML with pagination links."""
    return """
<!DOCTYPE html>
<html>
<head><title>Member Directory</title></head>
<body>
    <h1>Our Members</h1>
    <div class="member-list">
        <div class="member-item">
            <a href="/member/acme-corp" class="member-link">Acme Manufacturing Corp</a>
            <p class="location">Detroit, MI</p>
        </div>
        <div class="member-item">
            <a href="/member/beta-industries" class="member-link">Beta Industries LLC</a>
            <p class="location">Chicago, IL</p>
        </div>
        <div class="member-item">
            <a href="/member/gamma-systems" class="member-link">Gamma Systems Inc</a>
            <p class="location">Cleveland, OH</p>
        </div>
    </div>
    <div class="pagination">
        <a href="?page=1">1</a>
        <a href="?page=2">2</a>
        <a href="?page=3">3</a>
        <a href="?page=2">Next</a>
    </div>
</body>
</html>
"""


@pytest.fixture
def member_directory_infinite_scroll_html() -> str:
    """Member directory HTML with infinite scroll."""
    return """
<!DOCTYPE html>
<html>
<head><title>Member Directory</title></head>
<body>
    <h1>Our Members</h1>
    <div class="member-list" data-infinite-scroll="true" data-next-page="/members?page=2">
        <div class="member-item">
            <a href="/member/acme-corp">Acme Manufacturing Corp</a>
        </div>
        <div class="member-item">
            <a href="/member/beta-industries">Beta Industries LLC</a>
        </div>
    </div>
</body>
</html>
"""


@pytest.fixture
def member_directory_load_more_html() -> str:
    """Member directory HTML with Load More button."""
    return """
<!DOCTYPE html>
<html>
<head><title>Member Directory</title></head>
<body>
    <h1>Our Members</h1>
    <div class="member-list">
        <div class="member-item">
            <a href="/member/acme-corp">Acme Manufacturing Corp</a>
        </div>
        <div class="member-item">
            <a href="/member/beta-industries">Beta Industries LLC</a>
        </div>
    </div>
    <button class="load-more-btn">Load More</button>
</body>
</html>
"""


@pytest.fixture
def login_required_page_html() -> str:
    """Page that requires login to access."""
    return """
<!DOCTYPE html>
<html>
<head><title>Login Required</title></head>
<body>
    <div class="auth-notice">
        <h1>Members Only</h1>
        <p>Please log in to view this content.</p>
        <form action="/login" method="POST">
            <input type="email" name="email" placeholder="Email">
            <input type="password" name="password" placeholder="Password">
            <button type="submit">Login</button>
        </form>
    </div>
</body>
</html>
"""


@pytest.fixture
def paywall_page_html() -> str:
    """Page with paywall/subscription requirement (no auth indicators)."""
    return """
<!DOCTYPE html>
<html>
<head><title>Article</title></head>
<body>
    <div class="paywall-notice">
        <h1>Article Title</h1>
        <p>Subscribe to continue reading this article.</p>
        <button>Upgrade Your Plan</button>
    </div>
</body>
</html>
"""


@pytest.fixture
def tos_with_scraping_restriction() -> str:
    """Terms of Service with scraping restrictions."""
    return """
<!DOCTYPE html>
<html>
<head><title>Terms of Service</title></head>
<body>
    <h1>Terms of Service</h1>
    <h2>1. Use of Service</h2>
    <p>You agree to use our service responsibly.</p>

    <h2>2. Prohibited Activities</h2>
    <p>The following activities are prohibited:</p>
    <ul>
        <li>Scraping is prohibited on this website.</li>
        <li>Automated access and data collection is forbidden.</li>
        <li>Use of bots or crawlers is not allowed.</li>
    </ul>

    <h2>3. Data Protection</h2>
    <p>We protect your personal data according to privacy laws.</p>
</body>
</html>
"""


@pytest.fixture
def tos_without_restrictions() -> str:
    """Terms of Service without crawling restrictions."""
    return """
<!DOCTYPE html>
<html>
<head><title>Terms of Service</title></head>
<body>
    <h1>Terms of Service</h1>
    <h2>1. Use of Service</h2>
    <p>You agree to use our service responsibly.</p>

    <h2>2. User Responsibilities</h2>
    <p>Users must comply with all applicable laws.</p>

    <h2>3. Intellectual Property</h2>
    <p>All content is protected by copyright.</p>

    <h2>4. Privacy</h2>
    <p>We protect your personal data according to our privacy policy.</p>
</body>
</html>
"""


@pytest.fixture
def event_page_html() -> str:
    """Event listing page HTML."""
    return """
<!DOCTYPE html>
<html>
<head><title>Upcoming Events - PMA</title></head>
<body>
    <h1>Upcoming Events</h1>
    <div class="event-list">
        <div class="event-item">
            <h3>FABTECH 2024</h3>
            <p class="event-date">October 15-17, 2024</p>
            <p class="event-location">Orlando, FL</p>
            <a href="/events/fabtech-2024">Learn More</a>
        </div>
        <div class="event-item">
            <h3>Annual Meeting 2024</h3>
            <p class="event-date">March 5, 2024</p>
            <p class="event-location">Chicago, IL</p>
            <a href="/events/annual-meeting-2024">Learn More</a>
        </div>
        <div class="event-item">
            <h3>Webinar: Industry Trends</h3>
            <p class="event-date">February 28, 2024</p>
            <p class="event-location">Online</p>
            <a href="/events/webinar-trends">Register</a>
        </div>
    </div>
</body>
</html>
"""


@pytest.fixture
def sponsors_page_html() -> str:
    """Sponsors listing page HTML."""
    return """
<!DOCTYPE html>
<html>
<head><title>Our Sponsors - FABTECH 2024</title></head>
<body>
    <h1>Thank You to Our Sponsors</h1>

    <div class="sponsor-tier platinum">
        <h2>Platinum Sponsors</h2>
        <div class="sponsor-item">
            <a href="https://sponsor1.com">Sponsor One Inc</a>
        </div>
        <div class="sponsor-item">
            <a href="https://sponsor2.com">Sponsor Two Corp</a>
        </div>
    </div>

    <div class="sponsor-tier gold">
        <h2>Gold Sponsors</h2>
        <div class="sponsor-item">
            <a href="https://sponsor3.com">Sponsor Three LLC</a>
        </div>
    </div>

    <div class="sponsor-tier silver">
        <h2>Silver Sponsors</h2>
        <div class="sponsor-item">
            <a href="https://sponsor4.com">Sponsor Four Ltd</a>
        </div>
    </div>
</body>
</html>
"""


@pytest.fixture
def exhibitors_page_html() -> str:
    """Exhibitors listing page HTML."""
    return """
<!DOCTYPE html>
<html>
<head><title>Exhibitors - FABTECH 2024</title></head>
<body>
    <h1>Exhibitor Directory</h1>
    <p>Browse our exhibitors by booth number or company name.</p>

    <div class="exhibitor-list">
        <div class="exhibitor-item">
            <span class="booth-number">Booth 101</span>
            <a href="/exhibitor/acme" class="company-name">Acme Manufacturing</a>
            <span class="category">CNC Machines</span>
        </div>
        <div class="exhibitor-item">
            <span class="booth-number">Booth 102</span>
            <a href="/exhibitor/beta" class="company-name">Beta Industries</a>
            <span class="category">Welding Equipment</span>
        </div>
        <div class="exhibitor-item">
            <span class="booth-number">Booth 103</span>
            <a href="/exhibitor/gamma" class="company-name">Gamma Systems</a>
            <span class="category">Automation</span>
        </div>
    </div>
</body>
</html>
"""


# =============================================================================
# PDF PARSER FIXTURES
# =============================================================================


@pytest.fixture
def sample_pdf_table():
    """Table data with Company/City/State/Phone headers."""
    return [
        ["Company Name", "City", "State", "Phone"],
        ["Acme Manufacturing Inc", "Detroit", "MI", "(555) 123-4567"],
        ["Beta Industries LLC", "Chicago", "IL", "(555) 987-6543"],
        ["Gamma Systems Corp", "Cleveland", "OH", "(555) 456-7890"],
    ]


@pytest.fixture
def sample_pdf_text():
    """Multi-block text with company entries."""
    return """Acme Manufacturing Inc
Detroit, MI 48201
(555) 123-4567
info@acme-mfg.com
www.acme-mfg.com

Beta Industries LLC
Chicago, IL 60601
(555) 987-6543
contact@beta-ind.com

Gamma Systems Corp
Cleveland, OH 44101
(555) 456-7890
"""


@pytest.fixture
def sample_pdf_header_variations():
    """All 22 header->field mappings for parametrized tests."""
    return {
        "company": "company_name",
        "company name": "company_name",
        "name": "company_name",
        "member": "company_name",
        "member name": "company_name",
        "organization": "company_name",
        "city": "city",
        "state": "state",
        "st": "state",
        "province": "state",
        "country": "country",
        "phone": "phone",
        "telephone": "phone",
        "email": "email",
        "e-mail": "email",
        "website": "website",
        "web": "website",
        "url": "website",
        "membership": "membership_tier",
        "membership type": "membership_tier",
        "type": "membership_tier",
        "joined": "member_since",
        "member since": "member_since",
        "year joined": "member_since",
    }


@pytest.fixture
def mock_pdfplumber_pdf():
    """Factory fixture creating mocked pdfplumber PDF objects."""
    def _create(pages_data=None):
        mock_pdf = MagicMock()
        pages = []
        for page_data in (pages_data or []):
            mock_page = MagicMock()
            mock_page.extract_tables.return_value = page_data.get("tables", [])
            mock_page.extract_text.return_value = page_data.get("text", "")
            pages.append(mock_page)
        mock_pdf.pages = pages
        mock_pdf.__enter__ = MagicMock(return_value=mock_pdf)
        mock_pdf.__exit__ = MagicMock(return_value=False)
        return mock_pdf
    return _create


# =============================================================================
# HTML PARSER EXTENDED FIXTURES
# =============================================================================


@pytest.fixture
def inline_directory_html():
    """HTML with mix of external, social, internal links for auto-extract tests."""
    return """
<!DOCTYPE html>
<html>
<head><title>Our Members</title></head>
<body>
    <h1>Member Companies</h1>
    <ul>
        <li><a href="https://acme-mfg.com">Acme Manufacturing Inc</a></li>
        <li><a href="https://www.beta-industries.com">Beta Industries*</a></li>
        <li><a href="https://gamma-systems.com">Gamma Systems Corp</a></li>
        <li><a href="https://www.facebook.com/pma">Follow us on Facebook</a></li>
        <li><a href="https://twitter.com/pma">Twitter</a></li>
        <li><a href="https://www.linkedin.com/company/pma">LinkedIn</a></li>
        <li><a href="/about">About Us</a></li>
        <li><a href="/contact">Contact</a></li>
        <li><a href="https://pma.org/events">Events</a></li>
        <li><a href="https://acme-mfg.com/products">Acme Products</a></li>
        <li><a href="https://delta-corp.com">Delta Corp</a></li>
        <li><a href="#">Empty Link</a></li>
        <li><a href="mailto:info@pma.org">Email Us</a></li>
        <li><a href="tel:+15551234567">Call Us</a></li>
        <li><a href="javascript:void(0)">JS Link</a></li>
        <li><a href="https://www.youtube.com/pma">YouTube</a></li>
    </ul>
</body>
</html>
"""


@pytest.fixture
def schema_with_mapping():
    """Schema dict with mapping config for field value translation."""
    return {
        "company_name": {"selectors": ["h1.company-name"]},
        "membership_tier": {
            "selectors": [".tier"],
            "mapping": {
                "P": "Platinum",
                "G": "Gold",
                "S": "Silver",
            }
        }
    }


@pytest.fixture
def schema_with_enum():
    """Schema dict with enum config for field validation."""
    return {
        "company_name": {"selectors": ["h1.company-name"]},
        "state": {
            "selectors": [".state"],
            "enum": ["MI", "OH", "IL", "IN", "PA"]
        }
    }
