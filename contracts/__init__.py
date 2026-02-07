"""
Contracts Module
NAM Intelligence Pipeline

JSON Schema contracts for agent I/O validation.
"""

from .validator import (
    ContractValidator,
    ContractValidationError,
    ContractPolicy,
    get_validator,
    validate_contract,
    validate_contract_strict,
)

__all__ = [
    "ContractValidator",
    "ContractValidationError",
    "ContractPolicy",
    "get_validator",
    "validate_contract",
    "validate_contract_strict",
]
