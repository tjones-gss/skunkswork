"""
Contract Validator
NAM Intelligence Pipeline

JSON Schema validation for agent inputs and outputs.
Provides decorators and utilities for enforcing I/O contracts.
"""

import functools
import json
import logging
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel

# Try to import jsonschema, fall back gracefully if not available
try:
    import jsonschema  # noqa: F401
    from jsonschema import Draft202012Validator
    from referencing import Registry, Resource
    from referencing.jsonschema import DRAFT202012
    JSONSCHEMA_AVAILABLE = True
except ImportError:
    JSONSCHEMA_AVAILABLE = False
    Draft202012Validator = None
    Registry = None
    Resource = None
    DRAFT202012 = None


logger = logging.getLogger(__name__)

T = TypeVar('T')

# Base path for contract schemas
CONTRACTS_DIR = Path(__file__).parent / "schemas"


class ContractValidationError(Exception):
    """Raised when contract validation fails."""

    def __init__(
        self,
        contract: str,
        direction: str,
        errors: list,
        agent: str = None,
        data: Any = None
    ):
        self.contract = contract
        self.direction = direction
        self.errors = errors
        self.agent = agent
        self.data = data

        error_summary = "; ".join(str(e) for e in errors[:3])
        if len(errors) > 3:
            error_summary += f" ... and {len(errors) - 3} more"

        super().__init__(
            f"Contract validation failed [{contract}] ({direction}): {error_summary}"
        )


class ContractValidator:
    """
    Validates data against JSON Schema contracts.

    Supports:
    - Core entity schemas (company, event, etc.)
    - Agent input/output contracts
    - Schema references ($ref)
    - Caching of loaded schemas
    """

    def __init__(self, contracts_dir: Path = None):
        self.contracts_dir = contracts_dir or CONTRACTS_DIR
        self._schema_cache: dict[str, dict] = {}
        self._validator_cache: dict[str, Any] = {}
        self._registry: Any | None = None  # Lazy-loaded referencing Registry

    def _load_schema(self, schema_path: str) -> dict:
        """Load a schema from file, using cache."""
        if schema_path in self._schema_cache:
            return self._schema_cache[schema_path]

        # Handle relative paths
        if not schema_path.endswith(".json"):
            schema_path = f"{schema_path}.json"

        full_path = self.contracts_dir / schema_path

        if not full_path.exists():
            raise FileNotFoundError(f"Contract schema not found: {full_path}")

        with open(full_path, encoding="utf-8") as f:
            schema = json.load(f)

        self._schema_cache[schema_path] = schema
        return schema

    def _build_registry(self) -> Any:
        """Build a referencing.Registry mapping $id URIs to schema resources.

        This enables resolution of $ref URIs (e.g., ./contact.json resolved
        relative to the schema's $id) when validating schemas.
        """
        if self._registry is not None:
            return self._registry

        resources = []
        schemas_dir = self.contracts_dir

        for schema_file in schemas_dir.rglob("*.json"):
            try:
                with open(schema_file, encoding="utf-8") as f:
                    schema = json.load(f)

                # Create resource - use auto-detect if $schema present,
                # otherwise explicitly use DRAFT202012
                if "$schema" in schema:
                    resource = Resource.from_contents(schema)
                else:
                    resource = DRAFT202012.create_resource(schema)

                if "$id" in schema:
                    resources.append((schema["$id"], resource))
                # Also register under file:// URI for relative resolution
                file_uri = f"file://{schema_file.as_posix()}"
                resources.append((file_uri, resource))
            except Exception as e:
                logger.warning(f"Failed to load schema {schema_file}: {e}")

        self._registry = Registry().with_resources(resources)
        return self._registry

    def _get_validator(self, schema_path: str) -> Any:
        """Get or create a validator for a schema."""
        if not JSONSCHEMA_AVAILABLE:
            logger.warning("jsonschema not installed, validation disabled")
            return None

        if schema_path in self._validator_cache:
            return self._validator_cache[schema_path]

        schema = self._load_schema(schema_path)

        # Build registry for $ref resolution
        registry = self._build_registry()

        validator = Draft202012Validator(schema, registry=registry)
        self._validator_cache[schema_path] = validator

        return validator

    def _normalize_agent_type(self, agent_type: str) -> str:
        """Convert agent type to schema path."""
        # e.g., "discovery.access_gatekeeper" -> "discovery/access_gatekeeper"
        return agent_type.replace(".", "/")

    def validate(
        self,
        schema_path: str,
        data: Any,
        raise_on_error: bool = True
    ) -> tuple[bool, list]:
        """
        Validate data against a schema.

        Args:
            schema_path: Path to schema relative to contracts/schemas/
            data: Data to validate
            raise_on_error: Whether to raise on validation failure

        Returns:
            Tuple of (is_valid, list of errors)
        """
        if not JSONSCHEMA_AVAILABLE:
            logger.warning("jsonschema not installed, skipping validation")
            return True, []

        validator = self._get_validator(schema_path)
        if validator is None:
            return True, []

        # Convert Pydantic models to dicts
        if isinstance(data, BaseModel):
            data = data.model_dump(mode="json")

        errors = list(validator.iter_errors(data))

        if errors and raise_on_error:
            raise ContractValidationError(
                contract=schema_path,
                direction="validate",
                errors=[e.message for e in errors],
                data=data
            )

        return len(errors) == 0, [e.message for e in errors]

    def validate_input(
        self,
        agent_type: str,
        data: dict,
        raise_on_error: bool = True
    ) -> tuple[bool, list]:
        """
        Validate agent input against its input contract.

        Args:
            agent_type: Agent type (e.g., "discovery.access_gatekeeper")
            data: Input data to validate
            raise_on_error: Whether to raise on validation failure

        Returns:
            Tuple of (is_valid, list of errors)
        """
        normalized = self._normalize_agent_type(agent_type)
        # Extract just the agent name for the schema file
        parts = agent_type.split(".")
        if len(parts) == 2:
            category, agent = parts
            schema_path = f"{category}/{agent}_input"
        else:
            schema_path = f"{normalized}_input"

        return self.validate(schema_path, data, raise_on_error)

    def validate_output(
        self,
        agent_type: str,
        data: dict,
        raise_on_error: bool = True
    ) -> tuple[bool, list]:
        """
        Validate agent output against its output contract.

        Args:
            agent_type: Agent type (e.g., "discovery.access_gatekeeper")
            data: Output data to validate
            raise_on_error: Whether to raise on validation failure

        Returns:
            Tuple of (is_valid, list of errors)
        """
        normalized = self._normalize_agent_type(agent_type)
        parts = agent_type.split(".")
        if len(parts) == 2:
            category, agent = parts
            schema_path = f"{category}/{agent}_output"
        else:
            schema_path = f"{normalized}_output"

        return self.validate(schema_path, data, raise_on_error)

    def validate_entity(
        self,
        entity_type: str,
        data: dict,
        raise_on_error: bool = True
    ) -> tuple[bool, list]:
        """
        Validate an entity against its core schema.

        Args:
            entity_type: Entity type (e.g., "company", "event")
            data: Entity data to validate
            raise_on_error: Whether to raise on validation failure

        Returns:
            Tuple of (is_valid, list of errors)
        """
        schema_path = f"core/{entity_type}"
        return self.validate(schema_path, data, raise_on_error)


# Global validator instance
_validator: ContractValidator | None = None


def get_validator() -> ContractValidator:
    """Get the global validator instance."""
    global _validator
    if _validator is None:
        _validator = ContractValidator()
    return _validator


def validate_contract[T](func: Callable[..., T]) -> Callable[..., T]:
    """
    Decorator to validate agent input and output against contracts.

    Expects the decorated function to be a method on a class with:
    - self.agent_type: str (e.g., "discovery.access_gatekeeper")

    Usage:
        class MyAgent(BaseAgent):
            @validate_contract
            async def run(self, task: dict) -> dict:
                ...
    """
    @functools.wraps(func)
    async def wrapper(self, task: dict, *args, **kwargs) -> dict:
        agent_type = getattr(self, 'agent_type', None)

        if not agent_type:
            logger.warning("No agent_type found, skipping contract validation")
            return await func(self, task, *args, **kwargs)

        validator = get_validator()

        # Validate input (don't raise, just log warnings)
        try:
            is_valid, errors = validator.validate_input(
                agent_type, task, raise_on_error=False
            )
            if not is_valid:
                logger.warning(
                    f"Input contract validation failed for {agent_type}: "
                    f"{errors[:3]}"
                )
        except FileNotFoundError:
            logger.debug(f"No input contract found for {agent_type}")
        except Exception as e:
            logger.warning(f"Input validation error for {agent_type}: {e}")

        # Execute the function
        result = await func(self, task, *args, **kwargs)

        # Validate output
        try:
            is_valid, errors = validator.validate_output(
                agent_type, result, raise_on_error=False
            )
            if not is_valid:
                logger.warning(
                    f"Output contract validation failed for {agent_type}: "
                    f"{errors[:3]}"
                )
        except FileNotFoundError:
            logger.debug(f"No output contract found for {agent_type}")
        except Exception as e:
            logger.warning(f"Output validation error for {agent_type}: {e}")

        return result

    return wrapper


def validate_contract_strict[T](func: Callable[..., T]) -> Callable[..., T]:
    """
    Strict version of validate_contract that raises on validation failure.

    Use this when you want to enforce contracts strictly.
    """
    @functools.wraps(func)
    async def wrapper(self, task: dict, *args, **kwargs) -> dict:
        agent_type = getattr(self, 'agent_type', None)

        if not agent_type:
            raise ValueError("agent_type required for strict contract validation")

        validator = get_validator()

        # Validate input (raises on error)
        validator.validate_input(agent_type, task, raise_on_error=True)

        # Execute the function
        result = await func(self, task, *args, **kwargs)

        # Validate output (raises on error)
        validator.validate_output(agent_type, result, raise_on_error=True)

        return result

    return wrapper


class ContractPolicy:
    """
    Policy enforcement for contract validation.

    Integrates with middleware/policy.py for centralized enforcement.
    """

    def __init__(self, strict: bool = False):
        self.strict = strict
        self.validator = get_validator()

    def enforce_input(self, agent_type: str, data: dict) -> dict:
        """
        Enforce input contract, potentially modifying data.

        Returns the (possibly modified) data.
        """
        try:
            is_valid, errors = self.validator.validate_input(
                agent_type, data, raise_on_error=self.strict
            )

            if not is_valid:
                logger.warning(
                    f"Input contract warning for {agent_type}: {errors[:3]}"
                )

                # Add validation metadata
                if "_contract_validation" not in data:
                    data["_contract_validation"] = {}
                data["_contract_validation"]["input_errors"] = errors

        except FileNotFoundError:
            pass  # No contract defined

        return data

    def enforce_output(self, agent_type: str, data: dict) -> dict:
        """
        Enforce output contract, potentially modifying data.

        Returns the (possibly modified) data.
        """
        try:
            is_valid, errors = self.validator.validate_output(
                agent_type, data, raise_on_error=self.strict
            )

            if not is_valid:
                logger.warning(
                    f"Output contract warning for {agent_type}: {errors[:3]}"
                )

                # Add validation metadata
                if "_contract_validation" not in data:
                    data["_contract_validation"] = {}
                data["_contract_validation"]["output_errors"] = errors

        except FileNotFoundError:
            pass  # No contract defined

        return data


# =============================================================================
# CLI INTERFACE
# =============================================================================

def main():
    """CLI for validating data against contracts."""
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description="Validate data against contract schemas"
    )
    parser.add_argument(
        "--schema",
        required=True,
        help="Schema path (e.g., 'core/company' or 'discovery/access_gatekeeper_input')"
    )
    parser.add_argument(
        "--file",
        required=True,
        help="JSON file to validate"
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with error code on validation failure"
    )

    args = parser.parse_args()

    # Load data
    try:
        with open(args.file, encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading file: {e}")
        sys.exit(1)

    # Validate
    validator = ContractValidator()

    try:
        is_valid, errors = validator.validate(
            args.schema, data, raise_on_error=args.strict
        )

        if is_valid:
            print(f"Validation PASSED for {args.schema}")
            sys.exit(0)
        else:
            print(f"Validation FAILED for {args.schema}")
            print("Errors:")
            for error in errors:
                print(f"  - {error}")
            sys.exit(1 if args.strict else 0)

    except ContractValidationError as e:
        print(f"Validation ERROR: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
