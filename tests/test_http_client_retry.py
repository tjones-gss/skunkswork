"""
Tests for AsyncHTTPClient 5xx retry, circuit breaker, and Prometheus metrics.

Phase 1: HTTP Client Hardening
Phase 2: Circuit Breaker (P2-T02) + Prometheus Metrics (P2-T01)
"""

import time
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

# =============================================================================
# HELPERS
# =============================================================================


def _make_response(status_code: int):
    """Create a mock httpx.Response with the given status code."""
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.headers = {}
    response.request = MagicMock()
    return response


def _make_client_with_mock_responses(responses):
    """Create an AsyncHTTPClient with a mocked internal client.

    responses: list of mock responses, or side_effect for request mock.
    """
    from skills.common.SKILL import AsyncHTTPClient, RateLimiter

    client = AsyncHTTPClient(RateLimiter())

    mock_inner = AsyncMock()
    mock_inner.is_closed = False

    if isinstance(responses, list):
        mock_inner.request = AsyncMock(side_effect=responses)
    else:
        mock_inner.request = AsyncMock(side_effect=responses)

    client._client = mock_inner
    return client, mock_inner


# =============================================================================
# TEST RETRYABLE STATUS CODES
# =============================================================================


class TestRetryableStatusCodes:
    """Tests that the correct status codes trigger retry."""

    def test_retryable_set_contains_expected_codes(self):
        """RETRYABLE_STATUS_CODES includes 500, 502, 503, 504."""
        from skills.common.SKILL import AsyncHTTPClient

        assert 500 in AsyncHTTPClient.RETRYABLE_STATUS_CODES
        assert 502 in AsyncHTTPClient.RETRYABLE_STATUS_CODES
        assert 503 in AsyncHTTPClient.RETRYABLE_STATUS_CODES
        assert 504 in AsyncHTTPClient.RETRYABLE_STATUS_CODES

    def test_retryable_set_excludes_4xx(self):
        """RETRYABLE_STATUS_CODES excludes client errors."""
        from skills.common.SKILL import AsyncHTTPClient

        assert 400 not in AsyncHTTPClient.RETRYABLE_STATUS_CODES
        assert 403 not in AsyncHTTPClient.RETRYABLE_STATUS_CODES
        assert 404 not in AsyncHTTPClient.RETRYABLE_STATUS_CODES
        assert 422 not in AsyncHTTPClient.RETRYABLE_STATUS_CODES

    def test_max_backoff_defined(self):
        """MAX_BACKOFF constant is defined."""
        from skills.common.SKILL import AsyncHTTPClient

        assert AsyncHTTPClient.MAX_BACKOFF == 60


# =============================================================================
# TEST 5xx RETRY BEHAVIOR
# =============================================================================


class TestHTTPClient5xxRetry:
    """Tests for 5xx retry behavior."""

    @pytest.mark.asyncio
    async def test_502_triggers_retry(self):
        """502 response triggers retry and succeeds on second attempt."""
        client, mock_inner = _make_client_with_mock_responses([
            _make_response(502), _make_response(200),
        ])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request("GET", "https://example.com/api")

        assert response.status_code == 200
        assert mock_inner.request.call_count == 2

    @pytest.mark.asyncio
    async def test_503_triggers_retry(self):
        """503 response triggers retry."""
        client, mock_inner = _make_client_with_mock_responses([
            _make_response(503), _make_response(200),
        ])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request("GET", "https://example.com/api")

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_504_triggers_retry(self):
        """504 response triggers retry."""
        client, mock_inner = _make_client_with_mock_responses([
            _make_response(504), _make_response(200),
        ])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request("GET", "https://example.com/api")

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_500_triggers_retry(self):
        """500 response triggers retry."""
        client, mock_inner = _make_client_with_mock_responses([
            _make_response(500), _make_response(200),
        ])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request("GET", "https://example.com/api")

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_5xx_exhausts_retries_returns_last_response(self):
        """All retries exhausted on 5xx returns last response (not raise)."""
        client, mock_inner = _make_client_with_mock_responses([
            _make_response(502), _make_response(502),
            _make_response(502), _make_response(502),
        ])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request("GET", "https://example.com/api")

        # Should return the last 502 response, not raise
        assert response.status_code == 502

    @pytest.mark.asyncio
    async def test_4xx_does_not_retry(self):
        """4xx responses are NOT retried."""
        client, mock_inner = _make_client_with_mock_responses([
            _make_response(404),
        ])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request("GET", "https://example.com/api")

        assert response.status_code == 404
        assert mock_inner.request.call_count == 1  # No retries

    @pytest.mark.asyncio
    async def test_400_does_not_retry(self):
        """400 Bad Request is NOT retried."""
        client, mock_inner = _make_client_with_mock_responses([
            _make_response(400),
        ])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request("GET", "https://example.com/api")

        assert response.status_code == 400
        assert mock_inner.request.call_count == 1

    @pytest.mark.asyncio
    async def test_403_does_not_retry(self):
        """403 Forbidden is NOT retried."""
        client, mock_inner = _make_client_with_mock_responses([
            _make_response(403),
        ])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request("GET", "https://example.com/api")

        assert response.status_code == 403
        assert mock_inner.request.call_count == 1

    @pytest.mark.asyncio
    async def test_429_still_handled_by_retry_after(self):
        """429 is handled separately via Retry-After header, not via 5xx retry."""
        resp_429 = _make_response(429)
        resp_429.headers = {"Retry-After": "1"}

        client, mock_inner = _make_client_with_mock_responses([
            resp_429, _make_response(200),
        ])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request("GET", "https://example.com/api")

        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_mixed_503_then_200(self):
        """503 -> 503 -> 200 succeeds after two retries."""
        client, mock_inner = _make_client_with_mock_responses([
            _make_response(503), _make_response(503), _make_response(200),
        ])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request("GET", "https://example.com/api")

        assert response.status_code == 200
        assert mock_inner.request.call_count == 3


# =============================================================================
# TEST JITTER AND BACKOFF
# =============================================================================


class TestJitterAndBackoff:
    """Tests for backoff with jitter calculation."""

    @pytest.mark.asyncio
    async def test_backoff_capped_at_60s(self):
        """Backoff wait time is capped at MAX_BACKOFF (60s)."""
        client, _ = _make_client_with_mock_responses([
            _make_response(502), _make_response(200),
        ])

        sleep_values = []

        async def capture_sleep(duration):
            sleep_values.append(duration)

        with patch("asyncio.sleep", side_effect=capture_sleep):
            await client._request("GET", "https://example.com/api")

        # All sleep durations should be <= 60
        for val in sleep_values:
            assert val <= 60

    @pytest.mark.asyncio
    async def test_jitter_adds_randomness(self):
        """Backoff includes random jitter component."""
        sleep_values = []

        async def capture_sleep(duration):
            sleep_values.append(duration)

        # Run multiple times to check randomness
        for _ in range(5):
            client, _ = _make_client_with_mock_responses([
                _make_response(502), _make_response(200),
            ])

            with patch("asyncio.sleep", side_effect=capture_sleep):
                await client._request("GET", "https://example.com/api")

        # With jitter, sleep values should not all be identical
        if len(sleep_values) >= 2:
            assert len(set(round(v, 5) for v in sleep_values)) > 1

    @pytest.mark.asyncio
    async def test_connection_error_backoff_also_capped(self):
        """Connection error backoff is also capped at MAX_BACKOFF."""
        client, mock_inner = _make_client_with_mock_responses(
            httpx.ConnectError("Connection refused")
        )

        sleep_values = []

        async def capture_sleep(duration):
            sleep_values.append(duration)

        with patch("asyncio.sleep", side_effect=capture_sleep):
            with pytest.raises(httpx.ConnectError):
                await client._request("GET", "https://example.com/api")

        for val in sleep_values:
            assert val <= 60

    @pytest.mark.asyncio
    async def test_200_response_returns_immediately(self):
        """200 response returns without any retry sleep."""
        client, mock_inner = _make_client_with_mock_responses([
            _make_response(200),
        ])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request("GET", "https://example.com/api")

        assert response.status_code == 200
        assert mock_inner.request.call_count == 1

    @pytest.mark.asyncio
    async def test_custom_retries_zero_no_retry(self):
        """retries=0 means no retry on 5xx."""
        client, mock_inner = _make_client_with_mock_responses([
            _make_response(502),
        ])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request(
                "GET", "https://example.com/api", retries=0
            )

        assert response.status_code == 502
        assert mock_inner.request.call_count == 1


# =============================================================================
# TEST CIRCUIT BREAKER (P2-T02)
# =============================================================================


class TestCircuitBreakerUnit:
    """Unit tests for CircuitBreaker state machine."""

    def test_initial_state_is_closed(self):
        """New domain starts in CLOSED state."""
        from skills.common.SKILL import CircuitBreaker, CircuitState

        cb = CircuitBreaker()
        assert cb.get_state("example.com") == CircuitState.CLOSED

    def test_stays_closed_below_threshold(self):
        """Failures below threshold keep circuit CLOSED."""
        from skills.common.SKILL import CircuitBreaker, CircuitState

        cb = CircuitBreaker(failure_threshold=5)
        for _ in range(4):
            cb.record_failure("example.com")
        assert cb.get_state("example.com") == CircuitState.CLOSED

    def test_opens_at_threshold(self):
        """Circuit opens when failure count reaches threshold."""
        from skills.common.SKILL import CircuitBreaker, CircuitState

        cb = CircuitBreaker(failure_threshold=3)
        for _ in range(3):
            cb.record_failure("example.com")
        assert cb.get_state("example.com") == CircuitState.OPEN

    def test_open_raises_circuit_open_error(self):
        """check() raises CircuitOpenError when circuit is OPEN."""
        from skills.common.SKILL import CircuitBreaker, CircuitOpenError

        cb = CircuitBreaker(failure_threshold=1)
        cb.record_failure("example.com")
        with pytest.raises(CircuitOpenError) as exc_info:
            cb.check("example.com")
        assert exc_info.value.domain == "example.com"
        assert exc_info.value.reset_time >= 0

    def test_open_to_half_open_after_timeout(self):
        """Circuit transitions OPEN -> HALF_OPEN after reset_timeout."""
        from skills.common.SKILL import CircuitBreaker, CircuitState

        cb = CircuitBreaker(failure_threshold=1, reset_timeout=0.1)
        cb.record_failure("example.com")
        assert cb.get_state("example.com") == CircuitState.OPEN

        time.sleep(0.15)
        assert cb.get_state("example.com") == CircuitState.HALF_OPEN

    def test_half_open_success_closes(self):
        """Successful request in HALF_OPEN transitions to CLOSED."""
        from skills.common.SKILL import CircuitBreaker, CircuitState

        cb = CircuitBreaker(failure_threshold=1, reset_timeout=0.1)
        cb.record_failure("example.com")
        time.sleep(0.15)  # Wait for HALF_OPEN

        assert cb.get_state("example.com") == CircuitState.HALF_OPEN
        cb.check("example.com")  # Allowed (first half-open call)
        cb.record_success("example.com")
        assert cb.get_state("example.com") == CircuitState.CLOSED

    def test_half_open_failure_reopens(self):
        """Failed request in HALF_OPEN re-opens the circuit."""
        from skills.common.SKILL import CircuitBreaker, CircuitState

        cb = CircuitBreaker(failure_threshold=1, reset_timeout=0.1)
        cb.record_failure("example.com")
        time.sleep(0.15)

        assert cb.get_state("example.com") == CircuitState.HALF_OPEN
        cb.check("example.com")
        cb.record_failure("example.com")
        assert cb.get_state("example.com") == CircuitState.OPEN

    def test_half_open_limits_calls(self):
        """HALF_OPEN allows only half_open_max_calls before blocking."""
        from skills.common.SKILL import CircuitBreaker, CircuitOpenError

        cb = CircuitBreaker(failure_threshold=1, reset_timeout=0.1, half_open_max_calls=1)
        cb.record_failure("example.com")
        time.sleep(0.15)

        cb.check("example.com")  # First call allowed
        with pytest.raises(CircuitOpenError):
            cb.check("example.com")  # Second call blocked

    def test_reset_single_domain(self):
        """reset(domain) clears state for one domain."""
        from skills.common.SKILL import CircuitBreaker, CircuitState

        cb = CircuitBreaker(failure_threshold=1)
        cb.record_failure("a.com")
        cb.record_failure("b.com")
        assert cb.get_state("a.com") == CircuitState.OPEN

        cb.reset("a.com")
        assert cb.get_state("a.com") == CircuitState.CLOSED
        assert cb.get_state("b.com") == CircuitState.OPEN

    def test_reset_all_domains(self):
        """reset(None) clears state for all domains."""
        from skills.common.SKILL import CircuitBreaker, CircuitState

        cb = CircuitBreaker(failure_threshold=1)
        cb.record_failure("a.com")
        cb.record_failure("b.com")

        cb.reset()
        assert cb.get_state("a.com") == CircuitState.CLOSED
        assert cb.get_state("b.com") == CircuitState.CLOSED

    def test_success_resets_failure_count(self):
        """record_success() resets failure count so next failures start from 0."""
        from skills.common.SKILL import CircuitBreaker, CircuitState

        cb = CircuitBreaker(failure_threshold=3)
        cb.record_failure("example.com")
        cb.record_failure("example.com")
        cb.record_success("example.com")
        cb.record_failure("example.com")
        # Only 1 failure after success, should still be closed
        assert cb.get_state("example.com") == CircuitState.CLOSED

    def test_per_domain_isolation(self):
        """Circuit state is tracked independently per domain."""
        from skills.common.SKILL import CircuitBreaker, CircuitState

        cb = CircuitBreaker(failure_threshold=2)
        cb.record_failure("bad.com")
        cb.record_failure("bad.com")
        cb.record_failure("good.com")

        assert cb.get_state("bad.com") == CircuitState.OPEN
        assert cb.get_state("good.com") == CircuitState.CLOSED


class TestCircuitBreakerHTTPIntegration:
    """Integration tests: circuit breaker with AsyncHTTPClient."""

    @pytest.mark.asyncio
    async def test_open_circuit_skips_http(self):
        """Request to open circuit raises CircuitOpenError without making HTTP call."""
        from skills.common.SKILL import AsyncHTTPClient, CircuitBreaker, CircuitOpenError, RateLimiter

        cb = CircuitBreaker(failure_threshold=1)
        cb.record_failure("example.com")

        client = AsyncHTTPClient(RateLimiter(), circuit_breaker=cb)
        mock_inner = AsyncMock()
        mock_inner.is_closed = False
        client._client = mock_inner

        with pytest.raises(CircuitOpenError):
            await client._request("GET", "https://example.com/api")

        # HTTP client should NOT have been called
        assert mock_inner.request.call_count == 0

    @pytest.mark.asyncio
    async def test_5xx_trips_circuit_breaker(self):
        """5xx responses increment circuit breaker failure count."""
        from skills.common.SKILL import AsyncHTTPClient, CircuitBreaker, CircuitState, RateLimiter

        cb = CircuitBreaker(failure_threshold=2)
        client = AsyncHTTPClient(RateLimiter(), circuit_breaker=cb)

        mock_inner = AsyncMock()
        mock_inner.is_closed = False
        mock_inner.request = AsyncMock(return_value=_make_response(502))
        client._client = mock_inner

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await client._request("GET", "https://example.com/api", retries=0)

        # One 502 recorded, not yet at threshold
        assert cb.get_state("example.com") == CircuitState.CLOSED

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await client._request("GET", "https://example.com/api", retries=0)

        # Second 502 trips circuit
        assert cb.get_state("example.com") == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_429_does_not_trip_circuit(self):
        """429 rate limit does NOT trip circuit breaker."""
        from skills.common.SKILL import AsyncHTTPClient, CircuitBreaker, CircuitState, RateLimiter

        cb = CircuitBreaker(failure_threshold=1)
        client = AsyncHTTPClient(RateLimiter(), circuit_breaker=cb)

        resp_429 = _make_response(429)
        resp_429.headers = {"Retry-After": "0"}

        mock_inner = AsyncMock()
        mock_inner.is_closed = False
        mock_inner.request = AsyncMock(side_effect=[resp_429, _make_response(200)])
        client._client = mock_inner

        with patch("asyncio.sleep", new_callable=AsyncMock):
            response = await client._request("GET", "https://example.com/api")

        assert response.status_code == 200
        assert cb.get_state("example.com") == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_success_records_success_on_circuit(self):
        """Successful 2xx request calls record_success on circuit breaker."""
        from skills.common.SKILL import AsyncHTTPClient, CircuitBreaker, RateLimiter

        cb = CircuitBreaker(failure_threshold=5)
        # Pre-record some failures
        cb.record_failure("example.com")
        cb.record_failure("example.com")

        client = AsyncHTTPClient(RateLimiter(), circuit_breaker=cb)
        mock_inner = AsyncMock()
        mock_inner.is_closed = False
        mock_inner.request = AsyncMock(return_value=_make_response(200))
        client._client = mock_inner

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await client._request("GET", "https://example.com/api")

        # Failure count should be reset after success
        assert cb._failure_counts.get("example.com", 0) == 0


# =============================================================================
# TEST PROMETHEUS METRICS (P2-T01)
# =============================================================================


class TestPrometheusMetrics:
    """Tests for Prometheus metrics instrumentation in AsyncHTTPClient."""

    @pytest.mark.asyncio
    async def test_success_increments_request_counter(self):
        """Successful request increments nam_http_requests_total."""
        from skills.common.SKILL import HTTP_REQUESTS_TOTAL

        # Read counter before
        before = HTTP_REQUESTS_TOTAL.labels(
            domain="metrics-test-1.com", method="GET", status="200"
        )._value.get()

        client, _ = _make_client_with_mock_responses([_make_response(200)])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await client._request("GET", "https://metrics-test-1.com/api")

        after = HTTP_REQUESTS_TOTAL.labels(
            domain="metrics-test-1.com", method="GET", status="200"
        )._value.get()

        assert after - before == 1

    @pytest.mark.asyncio
    async def test_success_records_duration(self):
        """Successful request observes duration in histogram."""
        from skills.common.SKILL import HTTP_REQUEST_DURATION

        h = HTTP_REQUEST_DURATION.labels(domain="metrics-test-2.com", method="GET")

        client, _ = _make_client_with_mock_responses([_make_response(200)])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await client._request("GET", "https://metrics-test-2.com/api")

        # Check that an observation was recorded (count >= 1)
        sample_count = 0
        for sample in h.collect()[0].samples:
            if sample.name.endswith("_count"):
                sample_count = sample.value
        assert sample_count >= 1

    @pytest.mark.asyncio
    async def test_5xx_increments_both_request_and_error_counters(self):
        """5xx response increments both request and error counters."""
        from skills.common.SKILL import HTTP_ERRORS_TOTAL, HTTP_REQUESTS_TOTAL

        req_before = HTTP_REQUESTS_TOTAL.labels(
            domain="metrics-test-3.com", method="GET", status="502"
        )._value.get()
        err_before = HTTP_ERRORS_TOTAL.labels(
            domain="metrics-test-3.com", method="GET", error_type="http_502"
        )._value.get()

        client, _ = _make_client_with_mock_responses([_make_response(502)])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await client._request("GET", "https://metrics-test-3.com/api", retries=0)

        req_after = HTTP_REQUESTS_TOTAL.labels(
            domain="metrics-test-3.com", method="GET", status="502"
        )._value.get()
        err_after = HTTP_ERRORS_TOTAL.labels(
            domain="metrics-test-3.com", method="GET", error_type="http_502"
        )._value.get()

        assert req_after - req_before == 1
        assert err_after - err_before == 1

    @pytest.mark.asyncio
    async def test_429_does_not_increment_error_counter(self):
        """429 increments request counter but NOT error counter."""
        from skills.common.SKILL import HTTP_ERRORS_TOTAL, HTTP_REQUESTS_TOTAL

        # Use unique domain to avoid interference
        err_before = HTTP_ERRORS_TOTAL.labels(
            domain="metrics-test-4.com", method="GET", error_type="http_429"
        )._value.get()
        req_before = HTTP_REQUESTS_TOTAL.labels(
            domain="metrics-test-4.com", method="GET", status="429"
        )._value.get()

        resp_429 = _make_response(429)
        resp_429.headers = {"Retry-After": "0"}

        client, _ = _make_client_with_mock_responses([resp_429, _make_response(200)])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await client._request("GET", "https://metrics-test-4.com/api")

        err_after = HTTP_ERRORS_TOTAL.labels(
            domain="metrics-test-4.com", method="GET", error_type="http_429"
        )._value.get()
        req_after = HTTP_REQUESTS_TOTAL.labels(
            domain="metrics-test-4.com", method="GET", status="429"
        )._value.get()

        assert req_after - req_before == 1  # 429 tracked in requests
        assert err_after - err_before == 0  # NOT tracked in errors

    @pytest.mark.asyncio
    async def test_timeout_increments_error_counter(self):
        """Timeout exception increments error counter with class name."""
        from skills.common.SKILL import HTTP_ERRORS_TOTAL

        err_before = HTTP_ERRORS_TOTAL.labels(
            domain="metrics-test-5.com", method="GET", error_type="TimeoutException"
        )._value.get()

        client, _ = _make_client_with_mock_responses(
            httpx.TimeoutException("timed out")
        )

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(httpx.TimeoutException):
                await client._request("GET", "https://metrics-test-5.com/api")

        err_after = HTTP_ERRORS_TOTAL.labels(
            domain="metrics-test-5.com", method="GET", error_type="TimeoutException"
        )._value.get()

        # 4 errors: 1 initial + 3 retries
        assert err_after - err_before == 4

    @pytest.mark.asyncio
    async def test_connect_error_increments_error_counter(self):
        """ConnectError increments error counter with class name."""
        from skills.common.SKILL import HTTP_ERRORS_TOTAL

        err_before = HTTP_ERRORS_TOTAL.labels(
            domain="metrics-test-6.com", method="GET", error_type="ConnectError"
        )._value.get()

        client, _ = _make_client_with_mock_responses(
            httpx.ConnectError("connection refused")
        )

        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(httpx.ConnectError):
                await client._request("GET", "https://metrics-test-6.com/api")

        err_after = HTTP_ERRORS_TOTAL.labels(
            domain="metrics-test-6.com", method="GET", error_type="ConnectError"
        )._value.get()

        assert err_after - err_before == 4

    def test_get_metrics_text_returns_nonempty(self):
        """get_metrics_text() returns a non-empty string."""
        from skills.common.SKILL import get_metrics_text

        text = get_metrics_text()
        assert isinstance(text, str)
        assert len(text) > 0
        assert "nam_http_requests_total" in text

    @pytest.mark.asyncio
    async def test_post_method_tracked_correctly(self):
        """POST requests are tracked with method=POST."""
        from skills.common.SKILL import HTTP_REQUESTS_TOTAL

        before = HTTP_REQUESTS_TOTAL.labels(
            domain="metrics-test-7.com", method="POST", status="200"
        )._value.get()

        client, _ = _make_client_with_mock_responses([_make_response(200)])

        with patch("asyncio.sleep", new_callable=AsyncMock):
            await client._request("POST", "https://metrics-test-7.com/api")

        after = HTTP_REQUESTS_TOTAL.labels(
            domain="metrics-test-7.com", method="POST", status="200"
        )._value.get()

        assert after - before == 1
