"""
Middleware Package
NAM Intelligence Pipeline

Policy enforcement and cross-cutting concerns.
"""

from middleware.policy import (
    enforce_provenance,
    crawler_only,
    validate_json_output,
    ontology_labels_required,
    auth_pages_flagged,
    PolicyViolation,
)

__all__ = [
    "enforce_provenance",
    "crawler_only",
    "validate_json_output",
    "ontology_labels_required",
    "auth_pages_flagged",
    "PolicyViolation",
]
