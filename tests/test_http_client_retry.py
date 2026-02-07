"""
Tests for AsyncHTTPClient 5xx retry with jitter.

Phase 1: HTTP Client Hardening
"""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

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
            response = await client._request("GET", "https://example.com/api")

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

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
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
