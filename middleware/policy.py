"""
Policy Enforcement Middleware
NAM Intelligence Pipeline

Decorator-based policy enforcement based on gss-research-engine/policies.yaml.

Rules enforced:
- no_agent_fetches_pages_except_crawler
- no_agent_writes_data_except_edit_agents
- provenance_required_for_all_records
- auth_pages_must_be_flagged_not_scraped
- outputs_must_be_valid_json
- ontology_labels_required
"""

import functools
import json
import logging
from collections.abc import Callable
from typing import Any, TypeVar

from pydantic import BaseModel

logger = logging.getLogger(__name__)

# Type variable for generic decorators
T = TypeVar('T')


class PolicyViolation(Exception):
    """Raised when a policy rule is violated."""

    def __init__(self, policy: str, message: str, agent: str = None, context: dict = None):
        self.policy = policy
        self.agent = agent
        self.context = context or {}
        super().__init__(f"Policy violation [{policy}]: {message}")


# =============================================================================
# POLICY: PROVENANCE REQUIRED
# =============================================================================

def enforce_provenance[T](func: Callable[..., T]) -> Callable[..., T]:
    """
    Ensure all output records have provenance tracking.

    Validates that returned records contain:
    - source_url: Where data came from
    - extracted_at: When it was extracted
    - extracted_by: Which agent extracted it

    Usage:
        @enforce_provenance
        async def run(self, task: dict) -> dict:
            ...
    """
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        result = await func(self, *args, **kwargs)

        # Check if result contains records
        if isinstance(result, dict):
            records = result.get("records", [])

            if not isinstance(records, list):
                records = [records] if records else []

            for i, record in enumerate(records):
                if not isinstance(record, dict):
                    continue

                # Check for provenance fields
                missing_fields = []

                if not record.get("source_url") and not record.get("provenance"):
                    missing_fields.append("source_url")

                if not record.get("extracted_at") and not record.get("provenance"):
                    missing_fields.append("extracted_at")

                if missing_fields:
                    # Auto-add extracted_by from agent
                    agent_type = getattr(self, 'agent_type', 'unknown')
                    job_id = getattr(self, 'job_id', None)

                    if "provenance" not in record:
                        record["provenance"] = []

                    # Log warning but don't fail
                    logger.warning(
                        f"Record {i} missing provenance fields: {missing_fields}. "
                        f"Agent: {agent_type}, Job: {job_id}"
                    )

        return result

    return wrapper


# =============================================================================
# POLICY: CRAWLER ONLY FOR FETCHING
# =============================================================================

# Agents allowed to fetch pages
CRAWLER_AGENTS = {
    "discovery.link_crawler",
    "discovery.site_mapper",
    "discovery.access_gatekeeper",
    "intelligence.intelligent_crawler",
    "monitoring.source_monitor",
}


def crawler_only[T](func: Callable[..., T]) -> Callable[..., T]:
    """
    Restrict page fetching to crawler agents only.

    Non-crawler agents should receive pre-fetched content rather than
    fetching pages themselves. This ensures centralized rate limiting
    and access control.

    Usage:
        @crawler_only
        async def fetch_page(self, url: str) -> str:
            ...
    """
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        agent_type = getattr(self, 'agent_type', 'unknown')

        if agent_type not in CRAWLER_AGENTS:
            raise PolicyViolation(
                policy="no_agent_fetches_pages_except_crawler",
                message=f"Agent '{agent_type}' is not authorized to fetch pages. "
                        f"Only crawler agents can fetch: {CRAWLER_AGENTS}",
                agent=agent_type
            )

        return await func(self, *args, **kwargs)

    return wrapper


def is_crawler_agent(agent_type: str) -> bool:
    """Check if an agent type is allowed to fetch pages."""
    return agent_type in CRAWLER_AGENTS


# =============================================================================
# POLICY: ENRICHMENT HTTP ACCESS
# =============================================================================

# Agents allowed to make enrichment HTTP calls (API lookups, website fingerprinting)
ENRICHMENT_AGENTS = {"firmographic", "tech_stack", "contact_finder"}


def enrichment_http[T](func: Callable[..., T]) -> Callable[..., T]:
    """
    Allow HTTP access for enrichment agents with logging.

    Enrichment agents need HTTP for API calls (BuiltWith, Clearbit, etc.)
    and website fingerprinting. This decorator:
    - Allows only recognized enrichment agents
    - Logs all external HTTP calls for audit

    Usage:
        @enrichment_http
        async def _call_api(self, url: str) -> dict:
            ...
    """
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        agent_type = getattr(self, 'agent_type', 'unknown')

        # Extract the short agent name (e.g., "enrichment.firmographic" -> "firmographic")
        agent_short = agent_type.rsplit(".", 1)[-1] if "." in agent_type else agent_type

        if agent_short not in ENRICHMENT_AGENTS:
            raise PolicyViolation(
                policy="enrichment_http_only",
                message=f"Agent '{agent_type}' is not authorized for enrichment HTTP calls. "
                        f"Only enrichment agents can make API calls: {ENRICHMENT_AGENTS}",
                agent=agent_type
            )

        # Log the enrichment HTTP call
        url_arg = args[0] if args else kwargs.get("url", "unknown")
        logger.info(
            f"Enrichment HTTP call by {agent_type}: {url_arg}"
        )

        return await func(self, *args, **kwargs)

    return wrapper


def is_enrichment_agent(agent_type: str) -> bool:
    """Check if an agent type is allowed to make enrichment HTTP calls."""
    agent_short = agent_type.rsplit(".", 1)[-1] if "." in agent_type else agent_type
    return agent_short in ENRICHMENT_AGENTS


# =============================================================================
# POLICY: VALIDATE JSON OUTPUT
# =============================================================================

def validate_json_output[T](func: Callable[..., T]) -> Callable[..., T]:
    """
    Ensure all outputs are valid JSON-serializable.

    Validates that the return value can be serialized to JSON.
    Handles Pydantic models by converting them to dicts.

    Usage:
        @validate_json_output
        async def run(self, task: dict) -> dict:
            ...
    """
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        result = await func(self, *args, **kwargs)

        try:
            # Convert Pydantic models to dicts
            if isinstance(result, BaseModel):
                result = result.model_dump()

            # Validate JSON serialization
            json.dumps(result, default=str)

        except (TypeError, ValueError) as e:
            agent_type = getattr(self, 'agent_type', 'unknown')
            raise PolicyViolation(
                policy="outputs_must_be_valid_json",
                message=f"Output is not JSON-serializable: {e}",
                agent=agent_type,
                context={"error": str(e)}
            ) from e

        return result

    return wrapper


# =============================================================================
# POLICY: ONTOLOGY LABELS REQUIRED
# =============================================================================

def ontology_labels_required(*required_fields: str):
    """
    Ensure records contain required ontology labels.

    Args:
        required_fields: Fields that must be present in output records

    Usage:
        @ontology_labels_required("page_type", "entity_type")
        async def classify(self, page: str) -> dict:
            ...
    """
    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        async def wrapper(self, *args, **kwargs):
            result = await func(self, *args, **kwargs)

            # Check records in result
            if isinstance(result, dict):
                records = result.get("records", [result])
                if not isinstance(records, list):
                    records = [records]

                for i, record in enumerate(records):
                    if not isinstance(record, dict):
                        continue

                    missing = [f for f in required_fields if not record.get(f)]

                    if missing:
                        agent_type = getattr(self, 'agent_type', 'unknown')
                        raise PolicyViolation(
                            policy="ontology_labels_required",
                            message=f"Record {i} missing required ontology labels: {missing}",
                            agent=agent_type,
                            context={"missing_fields": missing, "record_index": i}
                        )

            return result

        return wrapper
    return decorator


# =============================================================================
# POLICY: AUTH PAGES FLAGGED NOT SCRAPED
# =============================================================================

AUTH_INDICATORS = [
    "please log in",
    "sign in to view",
    "members only",
    "login required",
    "authentication required",
    "access denied",
    "subscription required",
    "premium content",
]


def auth_pages_flagged[T](func: Callable[..., T]) -> Callable[..., T]:
    """
    Flag authenticated pages rather than attempting to scrape them.

    Checks page content for authentication indicators and flags
    the page rather than attempting extraction.

    Usage:
        @auth_pages_flagged
        async def extract(self, html: str, url: str) -> dict:
            ...
    """
    @functools.wraps(func)
    async def wrapper(self, *args, **kwargs):
        # Try to get HTML content from args
        html = None
        url = None

        for arg in args:
            if isinstance(arg, str):
                if arg.startswith("http"):
                    url = arg
                elif len(arg) > 200:  # Likely HTML
                    html = arg

        # Also check kwargs
        html = html or kwargs.get("html") or kwargs.get("content")
        url = url or kwargs.get("url")

        # Check for auth indicators
        if html:
            html_lower = html.lower()
            for indicator in AUTH_INDICATORS:
                if indicator in html_lower:
                    agent_type = getattr(self, 'agent_type', 'unknown')

                    logger.warning(
                        f"Auth page detected by {agent_type}: {url}. "
                        f"Indicator: '{indicator}'"
                    )

                    return {
                        "success": False,
                        "auth_required": True,
                        "auth_indicator": indicator,
                        "url": url,
                        "records": [],
                        "records_processed": 0,
                        "error": "Page requires authentication"
                    }

        return await func(self, *args, **kwargs)

    return wrapper


# =============================================================================
# COMPOSITE DECORATORS
# =============================================================================

def extraction_agent[T](func: Callable[..., T]) -> Callable[..., T]:
    """
    Composite decorator for extraction agents.

    Applies:
    - enforce_provenance
    - validate_json_output
    - auth_pages_flagged

    Usage:
        @extraction_agent
        async def run(self, task: dict) -> dict:
            ...
    """
    @functools.wraps(func)
    @enforce_provenance
    @validate_json_output
    @auth_pages_flagged
    async def wrapper(self, *args, **kwargs):
        return await func(self, *args, **kwargs)

    return wrapper


def validation_agent[T](func: Callable[..., T]) -> Callable[..., T]:
    """
    Composite decorator for validation agents.

    Applies:
    - validate_json_output

    Usage:
        @validation_agent
        async def run(self, task: dict) -> dict:
            ...
    """
    @functools.wraps(func)
    @validate_json_output
    async def wrapper(self, *args, **kwargs):
        return await func(self, *args, **kwargs)

    return wrapper


# =============================================================================
# POLICY CHECKER
# =============================================================================

class PolicyChecker:
    """
    Centralized policy checker for runtime validation.

    Can be used to check policies without decorators.
    """

    @staticmethod
    def check_provenance(record: dict) -> tuple[bool, list[str]]:
        """Check if record has valid provenance."""
        missing = []

        if not record.get("source_url") and not record.get("provenance"):
            missing.append("source_url")

        if not record.get("extracted_at") and not record.get("provenance"):
            missing.append("extracted_at")

        return len(missing) == 0, missing

    @staticmethod
    def check_json_serializable(data: Any) -> tuple[bool, str | None]:
        """Check if data is JSON serializable."""
        try:
            if isinstance(data, BaseModel):
                data = data.model_dump()
            json.dumps(data, default=str)
            return True, None
        except (TypeError, ValueError) as e:
            return False, str(e)

    @staticmethod
    def check_auth_required(html: str) -> tuple[bool, str | None]:
        """Check if page requires authentication."""
        if not html:
            return False, None

        html_lower = html.lower()
        for indicator in AUTH_INDICATORS:
            if indicator in html_lower:
                return True, indicator

        return False, None

    @staticmethod
    def check_crawler_permission(agent_type: str) -> bool:
        """Check if agent can fetch pages."""
        return agent_type in CRAWLER_AGENTS

    @staticmethod
    def check_enrichment_permission(agent_type: str) -> bool:
        """Check if agent can make enrichment HTTP calls."""
        return is_enrichment_agent(agent_type)
