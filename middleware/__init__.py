"""
Middleware Package
NAM Intelligence Pipeline

Policy enforcement and cross-cutting concerns.
"""

from middleware.policy import (
    PolicyViolation,
    auth_pages_flagged,
    crawler_only,
    enforce_provenance,
    ontology_labels_required,
    validate_json_output,
)

__all__ = [
    "enforce_provenance",
    "crawler_only",
    "validate_json_output",
    "ontology_labels_required",
    "auth_pages_flagged",
    "PolicyViolation",
]
