"""
Contract Validation Tests
NAM Intelligence Pipeline

Tests for contracts/validator.py - JSON Schema validation for agent I/O contracts.
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================================
# TEST: ContractValidator Initialization
# =============================================================================


class TestContractValidatorInitialization:
    """Tests for ContractValidator initialization."""

    def test_default_contracts_dir(self, project_root):
        """Validator uses default contracts directory."""
        from contracts.validator import ContractValidator, CONTRACTS_DIR

        validator = ContractValidator()
        assert validator.contracts_dir == CONTRACTS_DIR

    def test_custom_contracts_dir(self, contracts_dir):
        """Validator accepts custom contracts directory."""
        from contracts.validator import ContractValidator

        validator = ContractValidator(contracts_dir)
        assert validator.contracts_dir == contracts_dir

    def test_empty_cache_on_init(self, validator):
        """Validator starts with empty caches."""
        assert validator._schema_cache == {}
        assert validator._validator_cache == {}

    def test_contracts_dir_exists(self, contracts_dir):
        """Contracts directory exists in project."""
        assert contracts_dir.exists()
        assert contracts_dir.is_dir()


# =============================================================================
# TEST: Schema Loading
# =============================================================================


class TestSchemaLoading:
    """Tests for schema loading functionality."""

    def test_load_company_schema(self, validator):
        """Can load company schema."""
        schema = validator._load_schema("core/company.json")
        assert schema is not None
        assert schema.get("title") == "Company"
        assert "properties" in schema
        assert "company_name" in schema["properties"]

    def test_load_schema_without_extension(self, validator):
        """Can load schema without .json extension."""
        schema = validator._load_schema("core/company")
        assert schema is not None
        assert schema.get("title") == "Company"

    def test_schema_caching(self, validator):
        """Schemas are cached after first load."""
        # Load twice
        schema1 = validator._load_schema("core/company.json")
        schema2 = validator._load_schema("core/company.json")

        # Should be same object (cached)
        assert schema1 is schema2
        assert "core/company.json" in validator._schema_cache

    def test_missing_schema_raises_error(self, validator):
        """FileNotFoundError raised for missing schema."""
        with pytest.raises(FileNotFoundError) as exc_info:
            validator._load_schema("nonexistent/schema.json")

        assert "not found" in str(exc_info.value).lower()

    def test_load_all_core_schemas(self, validator, contracts_dir):
        """All core schemas can be loaded."""
        core_dir = contracts_dir / "core"
        core_schemas = list(core_dir.glob("*.json"))

        assert len(core_schemas) > 0, "No core schemas found"

        for schema_path in core_schemas:
            relative_path = f"core/{schema_path.name}"
            schema = validator._load_schema(relative_path)
            assert schema is not None
            assert "$schema" in schema or "properties" in schema

    def test_load_all_discovery_schemas(self, validator, contracts_dir):
        """All discovery schemas can be loaded."""
        discovery_dir = contracts_dir / "discovery"
        discovery_schemas = list(discovery_dir.glob("*.json"))

        assert len(discovery_schemas) > 0, "No discovery schemas found"

        for schema_path in discovery_schemas:
            relative_path = f"discovery/{schema_path.name}"
            schema = validator._load_schema(relative_path)
            assert schema is not None


# =============================================================================
# TEST: Core Entity Validation
# =============================================================================


class TestCoreEntityValidation:
    """Tests for core entity validation."""

    def test_valid_company(self, validator, valid_company):
        """Valid company passes validation."""
        is_valid, errors = validator.validate_entity("company", valid_company)
        assert is_valid is True
        assert errors == []

    def test_valid_company_minimal(self, validator):
        """Minimal valid company (only required fields)."""
        company = {"company_name": "Minimal Corp"}
        is_valid, errors = validator.validate_entity("company", company)
        assert is_valid is True

    def test_invalid_company_missing_name(self, validator, invalid_company_missing_name):
        """Company missing company_name fails validation."""
        is_valid, errors = validator.validate_entity(
            "company",
            invalid_company_missing_name,
            raise_on_error=False
        )
        assert is_valid is False
        assert len(errors) > 0
        assert any("company_name" in e.lower() or "required" in e.lower() for e in errors)

    def test_invalid_company_bad_score(self, validator, invalid_company_bad_score):
        """Company with quality_score > 100 fails validation."""
        is_valid, errors = validator.validate_entity(
            "company",
            invalid_company_bad_score,
            raise_on_error=False
        )
        assert is_valid is False
        assert len(errors) > 0

    def test_invalid_company_bad_grade(self, validator, invalid_company_bad_grade):
        """Company with invalid quality_grade fails validation."""
        is_valid, errors = validator.validate_entity(
            "company",
            invalid_company_bad_grade,
            raise_on_error=False
        )
        assert is_valid is False

    def test_valid_contact(self, validator, valid_contact):
        """Valid contact passes validation."""
        is_valid, errors = validator.validate_entity("contact", valid_contact)
        assert is_valid is True
        assert errors == []

    def test_valid_contact_minimal(self, validator):
        """Minimal valid contact (only required fields)."""
        contact = {"full_name": "Jane Doe"}
        is_valid, errors = validator.validate_entity("contact", contact)
        assert is_valid is True

    def test_invalid_contact_missing_name(self, validator, invalid_contact_missing_name):
        """Contact missing full_name fails validation."""
        is_valid, errors = validator.validate_entity(
            "contact",
            invalid_contact_missing_name,
            raise_on_error=False
        )
        assert is_valid is False
        assert len(errors) > 0

    def test_invalid_contact_bad_confidence(self, validator, invalid_contact_bad_confidence):
        """Contact with confidence_score > 1.0 fails validation."""
        is_valid, errors = validator.validate_entity(
            "contact",
            invalid_contact_bad_confidence,
            raise_on_error=False
        )
        assert is_valid is False

    def test_valid_event(self, validator, valid_event):
        """Valid event passes validation."""
        is_valid, errors = validator.validate_entity("event", valid_event)
        assert is_valid is True
        assert errors == []

    def test_valid_event_minimal(self, validator):
        """Minimal valid event (only required fields)."""
        event = {"title": "Annual Conference"}
        is_valid, errors = validator.validate_entity("event", event)
        assert is_valid is True

    def test_invalid_event_missing_title(self, validator):
        """Event missing title fails validation."""
        event = {"event_type": "CONFERENCE", "city": "Chicago"}
        is_valid, errors = validator.validate_entity("event", event, raise_on_error=False)
        assert is_valid is False

    def test_invalid_event_bad_type(self, validator):
        """Event with invalid event_type fails validation."""
        event = {"title": "Test Event", "event_type": "INVALID_TYPE"}
        is_valid, errors = validator.validate_entity("event", event, raise_on_error=False)
        assert is_valid is False

    def test_valid_provenance(self, validator, valid_provenance):
        """Valid provenance passes validation."""
        is_valid, errors = validator.validate_entity("provenance", valid_provenance)
        assert is_valid is True
        assert errors == []

    def test_valid_access_verdict(self, validator, valid_access_verdict):
        """Valid access_verdict passes validation."""
        is_valid, errors = validator.validate_entity("access_verdict", valid_access_verdict)
        assert is_valid is True
        assert errors == []


# =============================================================================
# TEST: Agent Contract Validation
# =============================================================================


class TestAgentContractValidation:
    """Tests for agent input/output contract validation."""

    def test_valid_gatekeeper_input(self, validator, valid_gatekeeper_input):
        """Valid gatekeeper input passes validation."""
        is_valid, errors = validator.validate_input(
            "discovery.access_gatekeeper",
            valid_gatekeeper_input
        )
        assert is_valid is True
        assert errors == []

    def test_valid_gatekeeper_output(self, validator, valid_gatekeeper_output):
        """Valid gatekeeper output passes validation."""
        is_valid, errors = validator.validate_output(
            "discovery.access_gatekeeper",
            valid_gatekeeper_output
        )
        assert is_valid is True
        assert errors == []

    def test_valid_gatekeeper_output_without_verdicts(self, validator):
        """Valid gatekeeper output (without $ref verdict items) passes validation."""
        output = {
            "success": True,
            "records_processed": 2,
            "verdicts": [],  # Empty to avoid $ref resolution
            "allowed_urls": ["https://www.pma.org/members"],
            "blocked_urls": []
        }
        is_valid, errors = validator.validate_output(
            "discovery.access_gatekeeper",
            output
        )
        assert is_valid is True
        assert errors == []

    def test_invalid_gatekeeper_input_empty_urls(self, validator, invalid_gatekeeper_input_empty_urls):
        """Gatekeeper input with empty urls array fails."""
        is_valid, errors = validator.validate_input(
            "discovery.access_gatekeeper",
            invalid_gatekeeper_input_empty_urls,
            raise_on_error=False
        )
        assert is_valid is False
        assert len(errors) > 0

    def test_invalid_gatekeeper_input_missing_urls(self, validator, invalid_gatekeeper_input_missing_urls):
        """Gatekeeper input missing urls field fails."""
        is_valid, errors = validator.validate_input(
            "discovery.access_gatekeeper",
            invalid_gatekeeper_input_missing_urls,
            raise_on_error=False
        )
        assert is_valid is False

    def test_valid_html_parser_input(self, validator, valid_html_parser_input):
        """Valid HTML parser input passes validation."""
        is_valid, errors = validator.validate_input(
            "extraction.html_parser",
            valid_html_parser_input
        )
        assert is_valid is True
        assert errors == []

    def test_normalize_agent_type(self, validator):
        """Agent type is normalized correctly."""
        normalized = validator._normalize_agent_type("discovery.access_gatekeeper")
        assert normalized == "discovery/access_gatekeeper"

    def test_invalid_gatekeeper_output_missing_required(self, validator):
        """Gatekeeper output missing required fields fails."""
        invalid_output = {"success": True}  # Missing records_processed and verdicts
        is_valid, errors = validator.validate_output(
            "discovery.access_gatekeeper",
            invalid_output,
            raise_on_error=False
        )
        assert is_valid is False


# =============================================================================
# TEST: $ref Resolution
# =============================================================================


class TestRefResolution:
    """Tests for JSON Schema $ref resolution.

    Note: Some tests are skipped because the current RefResolver configuration
    doesn't support resolving absolute $id URIs (https://nam-pipeline/...).
    For $ref resolution to work with the absolute IDs, we would need to
    register all schemas in a schema store. Tests with relative $refs that
    don't trigger the absolute $id resolution still work.
    """

    def test_company_with_contacts_ref(self, validator, valid_company, valid_contact):
        """Company with contacts array using $ref resolves correctly."""
        company = valid_company.copy()
        company["contacts"] = [valid_contact]

        is_valid, errors = validator.validate_entity("company", company)
        assert is_valid is True

    def test_company_with_invalid_contact_ref(self, validator, valid_company):
        """Company with invalid contact fails via $ref validation."""
        company = valid_company.copy()
        company["contacts"] = [{"email": "test@test.com"}]  # Missing full_name

        is_valid, errors = validator.validate_entity("company", company, raise_on_error=False)
        assert is_valid is False

    def test_company_with_provenance_ref(self, validator, valid_company, valid_provenance):
        """Company with provenance array using $ref resolves correctly."""
        company = valid_company.copy()
        company["provenance"] = [valid_provenance]

        is_valid, errors = validator.validate_entity("company", company)
        assert is_valid is True

    def test_company_with_invalid_provenance_ref(self, validator, valid_company):
        """Company with invalid provenance fails via $ref validation."""
        company = valid_company.copy()
        company["provenance"] = [{"source_url": "https://test.com"}]  # Missing extracted_by

        is_valid, errors = validator.validate_entity("company", company, raise_on_error=False)
        assert is_valid is False

    def test_gatekeeper_output_verdict_ref(self, validator, valid_access_verdict):
        """Gatekeeper output with verdicts array using $ref resolves correctly."""
        output = {
            "success": True,
            "records_processed": 1,
            "verdicts": [valid_access_verdict],
            "allowed_urls": ["https://test.com"],
            "blocked_urls": []
        }

        is_valid, errors = validator.validate_output(
            "discovery.access_gatekeeper",
            output
        )
        assert is_valid is True

    def test_gatekeeper_output_invalid_verdict_ref(self, validator):
        """Gatekeeper output with invalid verdict fails via $ref validation."""
        output = {
            "success": True,
            "records_processed": 1,
            "verdicts": [{"url": "https://test.com"}],  # Missing domain and is_allowed
            "allowed_urls": [],
            "blocked_urls": []
        }

        is_valid, errors = validator.validate_output(
            "discovery.access_gatekeeper",
            output,
            raise_on_error=False
        )
        assert is_valid is False

    def test_schema_contains_ref_syntax(self, validator):
        """Verify schemas contain $ref syntax (even if resolution is limited)."""
        company_schema = validator._load_schema("core/company.json")
        contacts_field = company_schema["properties"]["contacts"]

        assert "items" in contacts_field
        assert "$ref" in contacts_field["items"]
        assert contacts_field["items"]["$ref"] == "./contact.json"

    def test_provenance_ref_in_event_schema(self, validator):
        """Verify event schema contains provenance $ref."""
        event_schema = validator._load_schema("core/event.json")
        provenance_field = event_schema["properties"]["provenance"]

        assert "items" in provenance_field
        assert "$ref" in provenance_field["items"]


# =============================================================================
# TEST: raise_on_error Behavior
# =============================================================================


class TestRaiseOnError:
    """Tests for raise_on_error parameter behavior."""

    def test_raise_on_error_true_raises_exception(self, validator, invalid_company_missing_name):
        """raise_on_error=True raises ContractValidationError."""
        from contracts.validator import ContractValidationError

        with pytest.raises(ContractValidationError) as exc_info:
            validator.validate_entity(
                "company",
                invalid_company_missing_name,
                raise_on_error=True
            )

        error = exc_info.value
        assert error.contract == "core/company"
        assert error.direction == "validate"
        assert len(error.errors) > 0

    def test_raise_on_error_false_returns_tuple(self, validator, invalid_company_missing_name):
        """raise_on_error=False returns (is_valid, errors) tuple."""
        result = validator.validate_entity(
            "company",
            invalid_company_missing_name,
            raise_on_error=False
        )

        assert isinstance(result, tuple)
        assert len(result) == 2
        is_valid, errors = result
        assert is_valid is False
        assert isinstance(errors, list)

    def test_raise_on_error_default_is_true(self, validator, invalid_company_missing_name):
        """Default raise_on_error behavior is True."""
        from contracts.validator import ContractValidationError

        with pytest.raises(ContractValidationError):
            validator.validate_entity("company", invalid_company_missing_name)

    def test_validation_error_contains_data(self, validator, invalid_company_missing_name):
        """ContractValidationError includes the invalid data."""
        from contracts.validator import ContractValidationError

        with pytest.raises(ContractValidationError) as exc_info:
            validator.validate_entity(
                "company",
                invalid_company_missing_name,
                raise_on_error=True
            )

        error = exc_info.value
        assert error.data == invalid_company_missing_name

    def test_validation_error_message_format(self, validator, invalid_company_missing_name):
        """ContractValidationError has formatted message."""
        from contracts.validator import ContractValidationError

        with pytest.raises(ContractValidationError) as exc_info:
            validator.validate_entity(
                "company",
                invalid_company_missing_name,
                raise_on_error=True
            )

        error_msg = str(exc_info.value)
        assert "Contract validation failed" in error_msg
        assert "core/company" in error_msg


# =============================================================================
# TEST: Decorator Behavior
# =============================================================================


class TestValidateContractDecorators:
    """Tests for validate_contract and validate_contract_strict decorators."""

    @pytest.mark.asyncio
    async def test_lenient_decorator_logs_warning_but_continues(self):
        """validate_contract logs warnings but doesn't raise."""
        from contracts.validator import validate_contract

        class MockAgent:
            agent_type = "discovery.access_gatekeeper"

            @validate_contract
            async def run(self, task):
                return {"success": True, "records_processed": 0, "verdicts": []}

        agent = MockAgent()
        # Invalid input (missing urls)
        result = await agent.run({"check_robots": True})

        # Should complete despite invalid input
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_strict_decorator_raises_on_invalid_input(self):
        """validate_contract_strict raises on invalid input."""
        from contracts.validator import validate_contract_strict, ContractValidationError

        class MockAgent:
            agent_type = "discovery.access_gatekeeper"

            @validate_contract_strict
            async def run(self, task):
                return {"success": True, "records_processed": 0, "verdicts": []}

        agent = MockAgent()

        with pytest.raises(ContractValidationError):
            await agent.run({"check_robots": True})  # Missing required urls

    @pytest.mark.asyncio
    async def test_strict_decorator_raises_on_invalid_output(self):
        """validate_contract_strict raises on invalid output."""
        from contracts.validator import validate_contract_strict, ContractValidationError

        class MockAgent:
            agent_type = "discovery.access_gatekeeper"

            @validate_contract_strict
            async def run(self, task):
                return {"success": True}  # Missing required fields

        agent = MockAgent()

        with pytest.raises(ContractValidationError):
            await agent.run({
                "urls": ["https://test.com"],
                "check_robots": True
            })

    @pytest.mark.asyncio
    async def test_decorator_without_agent_type_skips_validation(self):
        """validate_contract skips validation if no agent_type."""
        from contracts.validator import validate_contract

        class MockAgent:
            # No agent_type attribute

            @validate_contract
            async def run(self, task):
                return {"result": "ok"}

        agent = MockAgent()
        result = await agent.run({"any": "data"})
        assert result["result"] == "ok"

    @pytest.mark.asyncio
    async def test_strict_decorator_without_agent_type_raises(self):
        """validate_contract_strict raises ValueError if no agent_type."""
        from contracts.validator import validate_contract_strict

        class MockAgent:
            # No agent_type attribute

            @validate_contract_strict
            async def run(self, task):
                return {"result": "ok"}

        agent = MockAgent()

        with pytest.raises(ValueError) as exc_info:
            await agent.run({"any": "data"})

        assert "agent_type required" in str(exc_info.value)


# =============================================================================
# TEST: ContractPolicy
# =============================================================================


class TestContractPolicy:
    """Tests for ContractPolicy class."""

    def test_enforce_input_adds_metadata_on_error(self, valid_gatekeeper_input):
        """ContractPolicy.enforce_input adds validation metadata on error."""
        from contracts.validator import ContractPolicy

        policy = ContractPolicy(strict=False)
        invalid_input = {"check_robots": True}  # Missing urls

        result = policy.enforce_input("discovery.access_gatekeeper", invalid_input)

        assert "_contract_validation" in result
        assert "input_errors" in result["_contract_validation"]
        assert len(result["_contract_validation"]["input_errors"]) > 0

    def test_enforce_input_strict_mode_raises(self):
        """ContractPolicy.enforce_input in strict mode raises on error."""
        from contracts.validator import ContractPolicy, ContractValidationError

        policy = ContractPolicy(strict=True)
        invalid_input = {"check_robots": True}  # Missing urls

        with pytest.raises(ContractValidationError):
            policy.enforce_input("discovery.access_gatekeeper", invalid_input)

    def test_enforce_output_adds_metadata_on_error(self):
        """ContractPolicy.enforce_output adds validation metadata on error."""
        from contracts.validator import ContractPolicy

        policy = ContractPolicy(strict=False)
        invalid_output = {"success": True}  # Missing required fields

        result = policy.enforce_output("discovery.access_gatekeeper", invalid_output)

        assert "_contract_validation" in result
        assert "output_errors" in result["_contract_validation"]

    def test_enforce_output_strict_mode_raises(self):
        """ContractPolicy.enforce_output in strict mode raises on error."""
        from contracts.validator import ContractPolicy, ContractValidationError

        policy = ContractPolicy(strict=True)
        invalid_output = {"success": True}

        with pytest.raises(ContractValidationError):
            policy.enforce_output("discovery.access_gatekeeper", invalid_output)

    def test_enforce_input_no_contract_passes_silently(self):
        """ContractPolicy.enforce_input passes silently when no contract exists."""
        from contracts.validator import ContractPolicy

        policy = ContractPolicy(strict=True)
        data = {"any": "data"}

        # Should not raise even in strict mode for unknown agent
        result = policy.enforce_input("nonexistent.agent", data)
        assert result == data

    def test_enforce_output_no_contract_passes_silently(self):
        """ContractPolicy.enforce_output passes silently when no contract exists."""
        from contracts.validator import ContractPolicy

        policy = ContractPolicy(strict=True)
        data = {"any": "data"}

        result = policy.enforce_output("nonexistent.agent", data)
        assert result == data


# =============================================================================
# TEST: Global Validator
# =============================================================================


class TestGlobalValidator:
    """Tests for global validator singleton."""

    def test_get_validator_returns_singleton(self):
        """get_validator returns the same instance."""
        from contracts.validator import get_validator
        import contracts.validator as validator_module

        # Reset for clean test
        validator_module._validator = None

        v1 = get_validator()
        v2 = get_validator()

        assert v1 is v2

    def test_get_validator_creates_instance(self):
        """get_validator creates ContractValidator if none exists."""
        from contracts.validator import get_validator, ContractValidator
        import contracts.validator as validator_module

        # Reset for clean test
        validator_module._validator = None

        v = get_validator()
        assert isinstance(v, ContractValidator)

    def test_global_validator_uses_default_path(self, global_validator, contracts_dir):
        """Global validator uses default contracts directory."""
        from contracts.validator import CONTRACTS_DIR

        assert global_validator.contracts_dir == CONTRACTS_DIR


# =============================================================================
# TEST: Pydantic Model Support
# =============================================================================


class TestPydanticModelSupport:
    """Tests for Pydantic model validation support."""

    def test_validate_pydantic_model(self, validator):
        """Can validate Pydantic models directly."""
        from pydantic import BaseModel

        class CompanyModel(BaseModel):
            company_name: str
            domain: str | None = None

        model = CompanyModel(company_name="Test Corp", domain="test.com")
        is_valid, errors = validator.validate_entity("company", model)

        assert is_valid is True

    def test_validate_invalid_pydantic_model(self, validator):
        """Invalid Pydantic model fails validation."""
        from pydantic import BaseModel

        class BadCompanyModel(BaseModel):
            domain: str | None = None
            # Missing company_name

        model = BadCompanyModel(domain="test.com")
        is_valid, errors = validator.validate_entity("company", model, raise_on_error=False)

        assert is_valid is False


# =============================================================================
# TEST: Schema Coverage
# =============================================================================


class TestSchemaCoverage:
    """Tests to verify all schemas are loadable and valid."""

    def test_all_extraction_schemas_loadable(self, validator, contracts_dir):
        """All extraction agent schemas can be loaded."""
        extraction_dir = contracts_dir / "extraction"
        if extraction_dir.exists():
            for schema_file in extraction_dir.glob("*.json"):
                schema = validator._load_schema(f"extraction/{schema_file.name}")
                assert schema is not None
                assert "$schema" in schema

    def test_all_enrichment_schemas_loadable(self, validator, contracts_dir):
        """All enrichment agent schemas can be loaded."""
        enrichment_dir = contracts_dir / "enrichment"
        if enrichment_dir.exists():
            for schema_file in enrichment_dir.glob("*.json"):
                schema = validator._load_schema(f"enrichment/{schema_file.name}")
                assert schema is not None

    def test_all_validation_schemas_loadable(self, validator, contracts_dir):
        """All validation agent schemas can be loaded."""
        validation_dir = contracts_dir / "validation"
        if validation_dir.exists():
            for schema_file in validation_dir.glob("*.json"):
                schema = validator._load_schema(f"validation/{schema_file.name}")
                assert schema is not None

    def test_schema_has_required_fields(self, validator):
        """Core schemas have expected structure."""
        company_schema = validator._load_schema("core/company.json")

        assert "type" in company_schema
        assert company_schema["type"] == "object"
        assert "properties" in company_schema
        assert "required" in company_schema
        assert "company_name" in company_schema["required"]


# =============================================================================
# TEST: CLI main() Function
# =============================================================================


class TestContractValidatorCLI:
    """Tests for the validator.py CLI main() function."""

    # -------------------------------------------------------------------------
    # Argument Parsing Tests
    # -------------------------------------------------------------------------

    def test_main_missing_schema_argument(self, tmp_path, capsys):
        """Test that missing --schema argument exits with error."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "data.json"
        test_file.write_text('{"company_name": "Test"}')

        with patch.object(sys, 'argv', ['validator.py', '--file', str(test_file)]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        # argparse exits with 2 for required argument missing
        assert exc_info.value.code == 2

    def test_main_missing_file_argument(self, capsys):
        """Test that missing --file argument exits with error."""
        import sys
        from unittest.mock import patch

        with patch.object(sys, 'argv', ['validator.py', '--schema', 'core/company']):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        assert exc_info.value.code == 2

    def test_main_strict_flag_parsed(self, tmp_path, capsys):
        """Test that --strict flag is recognized."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "valid.json"
        test_file.write_text('{"company_name": "Acme Corp"}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file),
            '--strict'
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        # Valid data should exit 0 even with strict
        assert exc_info.value.code == 0

    # -------------------------------------------------------------------------
    # File I/O Error Tests
    # -------------------------------------------------------------------------

    def test_main_file_not_found_exits_1(self, capsys):
        """Test that FileNotFoundError causes exit(1)."""
        import sys
        from unittest.mock import patch

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', '/nonexistent/path/file.json'
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Error loading file" in captured.out

    def test_main_invalid_json_exits_1(self, tmp_path, capsys):
        """Test that invalid JSON causes exit(1)."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "invalid.json"
        test_file.write_text('{not valid json}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Error loading file" in captured.out

    def test_main_file_error_message_printed(self, tmp_path, capsys):
        """Test that file error message is printed to stdout."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "bad.json"
        test_file.write_text('{"incomplete":')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit):
                from contracts.validator import main
                main()

        captured = capsys.readouterr()
        assert "Error" in captured.out

    # -------------------------------------------------------------------------
    # Validation Success Tests
    # -------------------------------------------------------------------------

    def test_main_valid_data_exits_0(self, tmp_path, capsys):
        """Test that valid data causes exit(0)."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "company.json"
        test_file.write_text('{"company_name": "Acme Corp"}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        assert exc_info.value.code == 0

    def test_main_success_message_printed(self, tmp_path, capsys):
        """Test that 'PASSED' is printed on success."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "company.json"
        test_file.write_text('{"company_name": "Test Company"}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit):
                from contracts.validator import main
                main()

        captured = capsys.readouterr()
        assert "PASSED" in captured.out

    def test_main_valid_strict_exits_0(self, tmp_path, capsys):
        """Test that valid data in strict mode also exits 0."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "valid_strict.json"
        test_file.write_text('{"company_name": "Test Corp"}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file),
            '--strict'
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "PASSED" in captured.out

    # -------------------------------------------------------------------------
    # Validation Failure - Non-Strict Mode Tests
    # -------------------------------------------------------------------------

    def test_main_invalid_data_nonstrict_exits_0(self, tmp_path, capsys):
        """Test that invalid data in non-strict mode exits 0."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "invalid_company.json"
        # Missing required company_name
        test_file.write_text('{"domain": "test.com"}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        # Non-strict mode exits 0 even on validation failure
        assert exc_info.value.code == 0

    def test_main_failure_message_printed(self, tmp_path, capsys):
        """Test that 'FAILED' is printed on validation failure."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "bad_company.json"
        test_file.write_text('{"domain": "test.com"}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit):
                from contracts.validator import main
                main()

        captured = capsys.readouterr()
        assert "FAILED" in captured.out

    def test_main_errors_listed(self, tmp_path, capsys):
        """Test that individual errors are printed."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "errors_company.json"
        test_file.write_text('{"domain": "test.com"}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit):
                from contracts.validator import main
                main()

        captured = capsys.readouterr()
        assert "Errors:" in captured.out
        # company_name is required
        assert "company_name" in captured.out.lower() or "required" in captured.out.lower()

    def test_main_nonstrict_is_default(self, tmp_path, capsys):
        """Test that non-strict mode is the default behavior."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "default_company.json"
        test_file.write_text('{}')  # Empty object, missing company_name

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        # Default (non-strict) should exit 0
        assert exc_info.value.code == 0

    # -------------------------------------------------------------------------
    # Validation Failure - Strict Mode Tests
    # -------------------------------------------------------------------------

    def test_main_invalid_data_strict_exits_1(self, tmp_path, capsys):
        """Test that invalid data in strict mode exits 1."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "strict_invalid.json"
        test_file.write_text('{"domain": "test.com"}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file),
            '--strict'
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        assert exc_info.value.code == 1

    def test_main_strict_error_output(self, tmp_path, capsys):
        """Test that error details are shown in strict mode."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "strict_error.json"
        test_file.write_text('{"invalid_field": "value"}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file),
            '--strict'
        ]):
            with pytest.raises(SystemExit):
                from contracts.validator import main
                main()

        captured = capsys.readouterr()
        # Should show error info
        assert "ERROR" in captured.out or "error" in captured.out.lower()

    def test_main_contract_validation_error_caught(self, tmp_path, capsys):
        """Test that ContractValidationError is handled properly in strict mode."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "contract_error.json"
        test_file.write_text('{"company_name": "Test", "quality_score": 150}')  # > max 100

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file),
            '--strict'
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        assert exc_info.value.code == 1

    # -------------------------------------------------------------------------
    # Edge Case Tests
    # -------------------------------------------------------------------------

    def test_main_schema_not_found(self, tmp_path, capsys):
        """Test that missing schema causes exit(1)."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "any.json"
        test_file.write_text('{"data": "value"}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'nonexistent/schema',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        assert exc_info.value.code == 1

    def test_main_empty_json_object(self, tmp_path, capsys):
        """Test that empty JSON object is processed."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "empty.json"
        test_file.write_text('{}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        # Empty object missing company_name, but non-strict exits 0
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "FAILED" in captured.out

    def test_main_empty_json_array(self, tmp_path, capsys):
        """Test that empty JSON array is processed."""
        import sys
        from unittest.mock import patch

        test_file = tmp_path / "empty_array.json"
        test_file.write_text('[]')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        # Array where object expected, non-strict exits 0
        assert exc_info.value.code == 0

    def test_main_unexpected_exception_exits_1(self, tmp_path, capsys):
        """Test that unexpected exceptions cause exit(1)."""
        import sys
        from unittest.mock import patch, MagicMock

        test_file = tmp_path / "exception.json"
        test_file.write_text('{"company_name": "Test"}')

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            # Patch ContractValidator to raise unexpected exception
            with patch('contracts.validator.ContractValidator') as mock_validator:
                mock_validator.return_value.validate.side_effect = RuntimeError("Unexpected!")
                with pytest.raises(SystemExit) as exc_info:
                    from contracts.validator import main
                    main()

        assert exc_info.value.code == 1
        captured = capsys.readouterr()
        assert "Unexpected" in captured.out

    def test_main_help_flag(self, capsys):
        """Test that --help works."""
        import sys
        from unittest.mock import patch

        with patch.object(sys, 'argv', ['validator.py', '--help']):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        # argparse exits with 0 for --help
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "Validate data against contract schemas" in captured.out

    def test_main_with_complex_valid_data(self, tmp_path, capsys):
        """Test validation with more complex valid company data."""
        import sys
        import json
        from unittest.mock import patch

        company_data = {
            "company_name": "Acme Manufacturing Inc",
            "domain": "acme.com",
            "website": "https://www.acme.com",
            "city": "Chicago",
            "state": "IL",
            "country": "United States",
            "employee_count_min": 100,
            "employee_count_max": 500,
            "tech_stack": ["SAP", "Salesforce"],
            "quality_score": 85
        }

        test_file = tmp_path / "complex_company.json"
        test_file.write_text(json.dumps(company_data))

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "PASSED" in captured.out

    def test_main_with_type_violation(self, tmp_path, capsys):
        """Test validation catches type violations."""
        import sys
        import json
        from unittest.mock import patch

        company_data = {
            "company_name": "Test Corp",
            "employee_count_min": "not a number"  # Should be integer
        }

        test_file = tmp_path / "type_error.json"
        test_file.write_text(json.dumps(company_data))

        with patch.object(sys, 'argv', [
            'validator.py',
            '--schema', 'core/company',
            '--file', str(test_file)
        ]):
            with pytest.raises(SystemExit) as exc_info:
                from contracts.validator import main
                main()

        # Non-strict exits 0
        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "FAILED" in captured.out
