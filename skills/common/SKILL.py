"""
Common Utilities - Python Implementation
NAM Intelligence Pipeline

Shared utilities, helpers, and constants used across all agents.
"""

import asyncio
import json
import logging
import logging.handlers
import os
import random
import re
import time
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import yaml
from prometheus_client import Counter, Histogram

# ============================================================================
# STATE CODES
# ============================================================================

STATE_CODES = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
    # Canadian provinces
    "ontario": "ON", "quebec": "QC", "british columbia": "BC", "alberta": "AB",
    "manitoba": "MB", "saskatchewan": "SK", "nova scotia": "NS",
    "new brunswick": "NB", "newfoundland": "NL", "prince edward island": "PE",
}

# Reverse lookup
STATE_NAMES = {v: k.title() for k, v in STATE_CODES.items()}


# ============================================================================
# RATE LIMITER
# ============================================================================

class RateLimiter:
    """Token bucket rate limiter per domain."""

    DEFAULT_RATE = 1.0  # requests per second

    RATE_LIMITS = {
        "default": 1.0,
        "pma.org": 0.5,
        "makeitelectric.org": 0.5,
        "socma.org": 0.5,
        "agma.org": 0.5,
        "aia-aerospace.org": 0.5,
        "ntma.org": 0.5,
        "pmpa.org": 0.5,
        "forging.org": 0.5,
        "diecasting.org": 0.5,
        "afsinc.org": 0.5,
        "linkedin.com": 0.2,
        "indeed.com": 0.3,
        "clearbit.com": 10.0,
        "builtwith.com": 5.0,
        "apollo.io": 0.8,
        "zoominfo.com": 1.5,
    }

    def __init__(self):
        self.last_request: dict[str, float] = {}
        self._lock = asyncio.Lock()

    def get_rate(self, domain: str) -> float:
        """Get rate limit for domain."""
        # Check exact match
        if domain in self.RATE_LIMITS:
            return self.RATE_LIMITS[domain]

        # Check if it's a subdomain
        for key, rate in self.RATE_LIMITS.items():
            if domain.endswith(f".{key}"):
                return rate

        return self.RATE_LIMITS["default"]

    async def acquire(self, domain: str):
        """Wait for rate limit and acquire slot."""
        async with self._lock:
            rate = self.get_rate(domain)
            min_interval = 1.0 / rate

            last = self.last_request.get(domain, 0)
            elapsed = time.time() - last

            if elapsed < min_interval:
                wait_time = min_interval - elapsed
                await asyncio.sleep(wait_time)

            self.last_request[domain] = time.time()


# ============================================================================
# CIRCUIT BREAKER
# ============================================================================

class CircuitState(Enum):
    """States for the circuit breaker."""
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitOpenError(Exception):
    """Raised when a request is attempted on an open circuit."""

    def __init__(self, domain: str, reset_time: float):
        self.domain = domain
        self.reset_time = reset_time
        super().__init__(
            f"Circuit open for {domain}, resets in {reset_time:.1f}s"
        )


class CircuitBreaker:
    """Per-domain circuit breaker to avoid hammering failing hosts."""

    def __init__(
        self,
        failure_threshold: int = 5,
        reset_timeout: float = 60.0,
        half_open_max_calls: int = 1,
    ):
        self.failure_threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.half_open_max_calls = half_open_max_calls

        self._states: dict[str, CircuitState] = {}
        self._failure_counts: dict[str, int] = {}
        self._last_failure_time: dict[str, float] = {}
        self._half_open_calls: dict[str, int] = {}

    def get_state(self, domain: str) -> CircuitState:
        """Get current state for domain, auto-transitioning OPEN -> HALF_OPEN."""
        state = self._states.get(domain, CircuitState.CLOSED)

        if state == CircuitState.OPEN:
            elapsed = time.time() - self._last_failure_time.get(domain, 0)
            if elapsed >= self.reset_timeout:
                self._states[domain] = CircuitState.HALF_OPEN
                self._half_open_calls[domain] = 0
                return CircuitState.HALF_OPEN

        return state

    def check(self, domain: str) -> None:
        """Check if request is allowed. Raises CircuitOpenError if OPEN."""
        state = self.get_state(domain)

        if state == CircuitState.OPEN:
            elapsed = time.time() - self._last_failure_time.get(domain, 0)
            remaining = max(0, self.reset_timeout - elapsed)
            raise CircuitOpenError(domain, remaining)

        if state == CircuitState.HALF_OPEN:
            if self._half_open_calls.get(domain, 0) >= self.half_open_max_calls:
                elapsed = time.time() - self._last_failure_time.get(domain, 0)
                remaining = max(0, self.reset_timeout - elapsed)
                raise CircuitOpenError(domain, remaining)
            self._half_open_calls[domain] = self._half_open_calls.get(domain, 0) + 1

    def record_success(self, domain: str) -> None:
        """Record a successful request. HALF_OPEN -> CLOSED."""
        state = self._states.get(domain, CircuitState.CLOSED)

        if state == CircuitState.HALF_OPEN:
            self._states[domain] = CircuitState.CLOSED

        self._failure_counts[domain] = 0

    def record_failure(self, domain: str) -> None:
        """Record a failed request. CLOSED -> OPEN at threshold, HALF_OPEN -> OPEN."""
        state = self._states.get(domain, CircuitState.CLOSED)
        self._failure_counts[domain] = self._failure_counts.get(domain, 0) + 1
        self._last_failure_time[domain] = time.time()

        if state == CircuitState.HALF_OPEN:
            self._states[domain] = CircuitState.OPEN
            return

        if self._failure_counts[domain] >= self.failure_threshold:
            self._states[domain] = CircuitState.OPEN

    def reset(self, domain: str | None = None) -> None:
        """Reset circuit breaker state for a domain, or all domains if None."""
        if domain is None:
            self._states.clear()
            self._failure_counts.clear()
            self._last_failure_time.clear()
            self._half_open_calls.clear()
        else:
            self._states.pop(domain, None)
            self._failure_counts.pop(domain, None)
            self._last_failure_time.pop(domain, None)
            self._half_open_calls.pop(domain, None)


# ============================================================================
# PROMETHEUS METRICS
# ============================================================================

HTTP_REQUESTS_TOTAL = Counter(
    "nam_http_requests_total",
    "Total HTTP requests by domain, method, and status",
    ["domain", "method", "status"],
)

HTTP_REQUEST_DURATION = Histogram(
    "nam_http_request_duration_seconds",
    "HTTP request duration in seconds",
    ["domain", "method"],
    buckets=[0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
)

HTTP_ERRORS_TOTAL = Counter(
    "nam_http_errors_total",
    "Total HTTP errors by domain, method, and error type",
    ["domain", "method", "error_type"],
)


def get_metrics_text() -> str:
    """Export all Prometheus metrics as text."""
    from prometheus_client import REGISTRY, generate_latest
    return generate_latest(REGISTRY).decode("utf-8")


# ============================================================================
# ASYNC HTTP CLIENT
# ============================================================================

class AsyncHTTPClient:
    """HTTP client with rate limiting, retries, and timeout handling."""

    DEFAULT_TIMEOUT = 30
    DEFAULT_RETRIES = 3
    DEFAULT_BACKOFF = 2.0
    MAX_BACKOFF = 60
    RETRYABLE_STATUS_CODES = {500, 502, 503, 504}

    # Use a browser-like UA to avoid WAF blocks on association websites.
    # The bot identifier is included as a secondary token per convention.
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 NAM-IntelBot/1.0"
    )

    def __init__(
        self,
        rate_limiter: RateLimiter = None,
        circuit_breaker: CircuitBreaker = None,
    ):
        self.rate_limiter = rate_limiter or RateLimiter()
        self.circuit_breaker = circuit_breaker or CircuitBreaker()
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.DEFAULT_TIMEOUT),
                headers={"User-Agent": self.USER_AGENT},
                follow_redirects=True,
            )
        return self._client

    async def close(self):
        """Close the HTTP client."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def get(
        self,
        url: str,
        params: dict = None,
        headers: dict = None,
        timeout: int = None,
        retries: int = None
    ) -> httpx.Response:
        """Make GET request with rate limiting and retries."""
        return await self._request("GET", url, params=params, headers=headers,
                                    timeout=timeout, retries=retries)

    async def post(
        self,
        url: str,
        json: dict = None,
        data: dict = None,
        headers: dict = None,
        timeout: int = None,
        retries: int = None
    ) -> httpx.Response:
        """Make POST request with rate limiting and retries."""
        return await self._request("POST", url, json=json, data=data,
                                    headers=headers, timeout=timeout, retries=retries)

    async def _request(
        self,
        method: str,
        url: str,
        params: dict = None,
        json: dict = None,
        data: dict = None,
        headers: dict = None,
        timeout: int = None,
        retries: int = None
    ) -> httpx.Response:
        """Make HTTP request with rate limiting, circuit breaker, and retries."""
        client = await self._get_client()
        domain = urlparse(url).netloc

        retries = retries if retries is not None else self.DEFAULT_RETRIES
        timeout = timeout if timeout is not None else self.DEFAULT_TIMEOUT

        # Circuit breaker check before retry loop
        self.circuit_breaker.check(domain)

        last_error = None

        for attempt in range(retries + 1):
            try:
                # Rate limit
                await self.rate_limiter.acquire(domain)

                # Make request with timing
                start_time = time.time()
                response = await client.request(
                    method,
                    url,
                    params=params,
                    json=json,
                    data=data,
                    headers=headers,
                    timeout=timeout,
                )
                duration = time.time() - start_time

                # Record metrics
                HTTP_REQUEST_DURATION.labels(domain=domain, method=method).observe(duration)
                HTTP_REQUESTS_TOTAL.labels(
                    domain=domain, method=method, status=str(response.status_code)
                ).inc()

                # Handle rate limiting (429 tracked in requests_total, NOT errors)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    await asyncio.sleep(retry_after)
                    continue

                # Handle retryable server errors (5xx)
                if response.status_code in self.RETRYABLE_STATUS_CODES:
                    HTTP_ERRORS_TOTAL.labels(
                        domain=domain, method=method,
                        error_type=f"http_{response.status_code}",
                    ).inc()
                    self.circuit_breaker.record_failure(domain)

                    last_error = httpx.HTTPStatusError(
                        f"Server error {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                    if attempt < retries:
                        wait = min(
                            self.DEFAULT_BACKOFF ** attempt + random.uniform(0, 1),
                            self.MAX_BACKOFF,
                        )
                        await asyncio.sleep(wait)
                        continue
                    return response  # Return last response if all retries exhausted

                # Success
                self.circuit_breaker.record_success(domain)
                return response

            except (httpx.TimeoutException, httpx.ConnectError) as e:
                duration = time.time() - start_time
                HTTP_REQUEST_DURATION.labels(domain=domain, method=method).observe(duration)
                HTTP_ERRORS_TOTAL.labels(
                    domain=domain, method=method,
                    error_type=type(e).__name__,
                ).inc()
                self.circuit_breaker.record_failure(domain)

                last_error = e
                if attempt < retries:
                    wait = min(
                        self.DEFAULT_BACKOFF ** attempt + random.uniform(0, 1),
                        self.MAX_BACKOFF,
                    )
                    await asyncio.sleep(wait)

        raise last_error or Exception(f"Request failed after {retries} retries")


# ============================================================================
# STRUCTURED LOGGER
# ============================================================================

class JsonFormatter(logging.Formatter):
    """Formats log records as single-line JSON for file output."""

    def format(self, record):
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if hasattr(record, "extra_fields"):
            log_entry.update(record.extra_fields)
        return json.dumps(log_entry, default=str)


class StructuredLogger:
    """JSON-structured logging for pipeline operations."""

    def __init__(self, agent_type: str, job_id: str = None):
        self.agent_type = agent_type
        self.job_id = job_id
        self._json_file_handler = None

        # Setup logger
        self.logger = logging.getLogger(f"nam_intel.{agent_type}")

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            ))
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)

    def setup_file_logging(
        self,
        log_dir: str = "data/logs",
        max_bytes: int = 10_485_760,
        backup_count: int = 5,
    ):
        """Add a rotating JSON file handler alongside the existing stdout handler."""
        log_path = Path(log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        file_handler = logging.handlers.RotatingFileHandler(
            log_path / f"{self.agent_type}.log",
            maxBytes=max_bytes,
            backupCount=backup_count,
        )
        file_handler.setFormatter(JsonFormatter())
        self.logger.addHandler(file_handler)
        self._json_file_handler = file_handler

    def _format(self, message: str, **kwargs) -> str:
        """Format message with context."""
        context = {
            "agent": self.agent_type,
            "job_id": self.job_id,
            **kwargs
        }
        context_str = " ".join(f"{k}={v}" for k, v in context.items() if v is not None)
        return f"{message} [{context_str}]"

    def _log(self, level: int, message: str, **kwargs):
        """Internal log method that attaches extra fields for JSON formatter."""
        formatted = self._format(message, **kwargs)
        extra_fields = {
            "agent": self.agent_type,
            "job_id": self.job_id,
            **{k: v for k, v in kwargs.items() if v is not None},
        }
        record = self.logger.makeRecord(
            self.logger.name, level, "(unknown)", 0, formatted, (), None
        )
        record.extra_fields = extra_fields
        self.logger.handle(record)

    def debug(self, message: str, **kwargs):
        """Log debug message."""
        self._log(logging.DEBUG, message, **kwargs)

    def info(self, message: str, **kwargs):
        """Log info message."""
        self._log(logging.INFO, message, **kwargs)

    def warning(self, message: str, **kwargs):
        """Log warning message."""
        self._log(logging.WARNING, message, **kwargs)

    def error(self, message: str, **kwargs):
        """Log error message."""
        self._log(logging.ERROR, message, **kwargs)


# ============================================================================
# JSONL WRITER/READER
# ============================================================================

class JSONLWriter:
    """Write records to JSONL file."""

    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = None

    def __enter__(self):
        self._file = open(self.path, "w", encoding="utf-8")
        return self

    def __exit__(self, *args):
        if self._file:
            self._file.close()

    def write(self, record: dict):
        """Write single record."""
        if self._file:
            self._file.write(json.dumps(record, ensure_ascii=False) + "\n")

    def write_batch(self, records: list[dict]):
        """Write batch of records."""
        for record in records:
            self.write(record)


class JSONLReader:
    """Read records from JSONL file."""

    def __init__(self, path: str):
        self.path = Path(path)

    def read_all(self) -> list[dict]:
        """Read all records from file."""
        records = []

        if not self.path.exists():
            return records

        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue

        return records

    def __iter__(self):
        """Iterate over records."""
        if not self.path.exists():
            return

        with open(self.path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        yield json.loads(line)
                    except json.JSONDecodeError:
                        continue


# ============================================================================
# CONFIG LOADER
# ============================================================================

class Config:
    """Configuration loader with environment variable substitution."""

    def __init__(self, config_path: str = "config"):
        self.config_path = Path(config_path)
        self._cache: dict[str, dict] = {}

    def load(self, name: str) -> dict:
        """Load configuration file by name."""
        if name in self._cache:
            return self._cache[name]

        # Try with .yaml extension
        path = self.config_path / f"{name}.yaml"
        if not path.exists():
            path = self.config_path / f"{name}.yml"

        if not path.exists():
            return {}

        with open(path, encoding="utf-8") as f:
            content = f.read()

        # Substitute environment variables
        content = self._substitute_env(content)

        config = yaml.safe_load(content) or {}
        self._cache[name] = config

        return config

    def _substitute_env(self, content: str) -> str:
        """Substitute ${VAR} with environment variables."""
        pattern = r'\$\{([^}]+)\}'

        def replacer(match):
            var_name = match.group(1)
            default = None

            if ":" in var_name:
                var_name, default = var_name.split(":", 1)

            return os.getenv(var_name, default or "")

        return re.sub(pattern, replacer, content)

    def get(self, key: str, default: Any = None) -> Any:
        """Get nested config value using dot notation."""
        parts = key.split(".")

        if parts:
            config = self.load(parts[0])
            for part in parts[1:]:
                if isinstance(config, dict):
                    config = config.get(part)
                else:
                    return default
            return config if config is not None else default

        return default


# ============================================================================
# NORMALIZATION UTILITIES
# ============================================================================

def normalize_company_name(name: str) -> str:
    """Normalize company name for matching."""
    if not name:
        return ""

    # Lowercase
    name = name.lower()

    # Remove common suffixes
    suffixes = [
        r'\b(inc\.?|incorporated|corp\.?|corporation|llc|l\.l\.c\.?)$',
        r'\b(ltd\.?|limited|co\.?|company|plc)$',
        r'\b(gmbh|ag|sa|nv|bv)$',
    ]

    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)

    # Remove punctuation
    name = re.sub(r'[^\w\s]', '', name)

    # Normalize whitespace
    name = ' '.join(name.split())

    return name.strip()


def normalize_url(url: str) -> str:
    """Normalize URL for consistent comparison."""
    if not url:
        return ""

    # Add protocol if missing
    if not url.startswith(('http://', 'https://')):
        url = f"https://{url}"

    # Parse and rebuild
    parsed = urlparse(url)

    # Lowercase domain
    domain = parsed.netloc.lower()

    # Remove www prefix
    if domain.startswith('www.'):
        domain = domain[4:]

    # Remove trailing slash from path
    path = parsed.path.rstrip('/')

    return f"{parsed.scheme}://{domain}{path}"


def extract_domain(url: str) -> str:
    """Extract clean domain from URL."""
    if not url:
        return ""

    if not url.startswith(('http://', 'https://')):
        url = f"https://{url}"

    parsed = urlparse(url)
    domain = parsed.netloc.lower()

    if domain.startswith('www.'):
        domain = domain[4:]

    return domain


# ============================================================================
# FIELD PARSERS
# ============================================================================

PARSERS = {
    "title_case": lambda s: s.strip().title() if s else None,

    "state_code": lambda s: (
        STATE_CODES.get(s.strip().lower(), s[:2].upper() if len(s) >= 2 else s)
        if s else None
    ),

    "year": lambda s: (
        int(re.search(r'(19|20)\d{2}', str(s)).group())
        if s and re.search(r'(19|20)\d{2}', str(s)) else None
    ),

    "phone": lambda s: re.sub(r'[^\d+]', '', s) if s else None,

    "email": lambda s: s.strip().lower() if s and '@' in s else None,

    "url": lambda s: (
        s if s and s.startswith('http')
        else f"https://{s}" if s else None
    ),

    "integer": lambda s: int(re.sub(r'[^\d]', '', str(s))) if s else None,

    # PMA-specific parsers: <cite> contains "City, ST"
    "pma_city": lambda s: s.rsplit(",", 1)[0].strip().title() if s and "," in s else None,

    "pma_state": lambda s: (
        s.rsplit(",", 1)[1].strip().upper()[:2] if s and "," in s else None
    ),

    # Extract member ID from PMA profile URL like profile.asp?id=00722807
    "pma_member_id": lambda s: (
        re.search(r'[?&]id=(\d+)', str(s)).group(1)
        if s and re.search(r'[?&]id=(\d+)', str(s)) else None
    ),
}


def apply_parser(value: str, parser_name: str) -> Any:
    """Apply a named parser to a value."""
    if parser_name in PARSERS:
        try:
            return PARSERS[parser_name](value)
        except (ValueError, TypeError, AttributeError):
            return value
    return value
