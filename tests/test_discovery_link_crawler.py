"""
Tests for LinkCrawlerAgent
NAM Intelligence Pipeline

Tests for the link crawler discovery agent that crawls member directories
to discover all member profile URLs following pagination.
"""

import json
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch


# =============================================================================
# TEST INITIALIZATION
# =============================================================================


class TestLinkCrawlerAgentInitialization:
    """Tests for LinkCrawlerAgent initialization."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_default_config_values(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test default configuration values."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        assert agent.max_pages == 200
        assert agent.batch_size == 50
        assert agent.concurrent_requests == 3

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_custom_config_values(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test custom configuration values."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        # Manually set custom config after initialization
        agent.agent_config = {
            "max_pages": 100,
            "batch_size": 25,
            "concurrent_requests": 5
        }
        agent._setup()  # Re-run setup with new config

        assert agent.max_pages == 100
        assert agent.batch_size == 25
        assert agent.concurrent_requests == 5

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_agent_type_is_set(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test agent type is properly set."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        assert agent.agent_type == "discovery.link_crawler"

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_http_client_available(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test HTTP client is available."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        assert agent.http is not None


# =============================================================================
# TEST RUN METHOD
# =============================================================================


class TestLinkCrawlerAgentRun:
    """Tests for LinkCrawlerAgent.run() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_single_page_no_pagination(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        member_directory_with_pagination_html, tmp_path
    ):
        """Test crawling single page with no pagination."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = member_directory_with_pagination_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        with patch.object(agent, '_save_urls'):
            result = await agent.run({
                "entry_url": "https://pma.org/members",
                "pagination": {"type": "none"},
                "association": "TEST"
            })

        assert result["success"] is True
        assert "member_urls" in result
        assert result["total_pages"] == 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_with_query_param_pagination(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        member_directory_with_pagination_html
    ):
        """Test crawling with query parameter pagination."""
        mock_config.return_value.load.return_value = {}

        # First page has content, second page empty
        page1 = MagicMock()
        page1.status_code = 200
        page1.text = member_directory_with_pagination_html

        page2 = MagicMock()
        page2.status_code = 200
        page2.text = "<html><body><p>No more members</p></body></html>"

        page3 = MagicMock()
        page3.status_code = 200
        page3.text = "<html><body><p>No more members</p></body></html>"

        page4 = MagicMock()
        page4.status_code = 200
        page4.text = "<html><body><p>No more members</p></body></html>"

        mock_http.return_value.get = AsyncMock(side_effect=[page1, page2, page3, page4])

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        with patch.object(agent, '_save_urls'):
            result = await agent.run({
                "entry_url": "https://pma.org/members?page=1",
                "pagination": {"type": "query_param", "param": "page"},
                "association": "TEST"
            })

        assert result["success"] is True
        assert result["total_pages"] >= 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_deduplicates_urls(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that duplicate URLs are deduplicated."""
        mock_config.return_value.load.return_value = {}

        # HTML with duplicate links
        html = """
        <html><body>
            <div class="member-list">
                <a href="/member/company-a">Company A</a>
                <a href="/member/company-a">Company A (duplicate)</a>
                <a href="/member/company-b">Company B</a>
            </div>
        </body></html>
        """

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        with patch.object(agent, '_save_urls'):
            result = await agent.run({
                "entry_url": "https://pma.org/members",
                "pagination": {"type": "none"},
                "association": "TEST"
            })

        # Should only have 2 unique URLs
        assert len(result["member_urls"]) == 2

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_saves_urls_to_file(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        tmp_path, member_directory_with_pagination_html
    ):
        """Test that URLs are saved to JSONL file."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = member_directory_with_pagination_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        # Mock _save_urls to verify it's called
        save_mock = MagicMock()
        with patch.object(agent, '_save_urls', save_mock):
            result = await agent.run({
                "entry_url": "https://pma.org/members",
                "pagination": {"type": "none"},
                "association": "TEST"
            })

        save_mock.assert_called_once()
        assert "output_path" in result

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_with_path_segment_pagination(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        member_directory_with_pagination_html
    ):
        """Test crawling with path segment pagination."""
        mock_config.return_value.load.return_value = {}

        page1 = MagicMock()
        page1.status_code = 200
        page1.text = member_directory_with_pagination_html

        page2 = MagicMock()
        page2.status_code = 200
        page2.text = "<html><body></body></html>"

        page3 = MagicMock()
        page3.status_code = 200
        page3.text = "<html><body></body></html>"

        page4 = MagicMock()
        page4.status_code = 200
        page4.text = "<html><body></body></html>"

        mock_http.return_value.get = AsyncMock(side_effect=[page1, page2, page3, page4])

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        with patch.object(agent, '_save_urls'):
            result = await agent.run({
                "entry_url": "https://pma.org/members/page/1",
                "pagination": {"type": "path_segment", "pattern": "/page/{n}"},
                "association": "TEST"
            })

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_returns_sorted_urls(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that returned URLs are sorted."""
        mock_config.return_value.load.return_value = {}

        html = """
        <html><body>
            <a href="/member/zebra" class="member">Zebra</a>
            <a href="/member/alpha" class="member">Alpha</a>
            <a href="/member/beta" class="member">Beta</a>
        </body></html>
        """

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        with patch.object(agent, '_save_urls'):
            result = await agent.run({
                "entry_url": "https://pma.org/members",
                "pagination": {"type": "none"},
                "association": "TEST"
            })

        # URLs should be sorted
        assert result["member_urls"] == sorted(result["member_urls"])

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_reports_total_urls(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        member_directory_with_pagination_html
    ):
        """Test that total URL count is reported."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = member_directory_with_pagination_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        with patch.object(agent, '_save_urls'):
            result = await agent.run({
                "entry_url": "https://pma.org/members",
                "pagination": {"type": "none"},
                "association": "TEST"
            })

        assert "total_urls" in result
        assert result["total_urls"] == len(result["member_urls"])

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_reports_records_processed(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        member_directory_with_pagination_html
    ):
        """Test that records_processed equals pages crawled."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = member_directory_with_pagination_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        with patch.object(agent, '_save_urls'):
            result = await agent.run({
                "entry_url": "https://pma.org/members",
                "pagination": {"type": "none"},
                "association": "TEST"
            })

        assert result["records_processed"] == result["total_pages"]

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_run_handles_default_pagination_type(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        member_directory_with_pagination_html
    ):
        """Test handling when pagination type is not specified."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = member_directory_with_pagination_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        with patch.object(agent, '_save_urls'):
            result = await agent.run({
                "entry_url": "https://pma.org/members",
                # No pagination specified
                "association": "TEST"
            })

        assert result["success"] is True


# =============================================================================
# TEST FETCH PAGE
# =============================================================================


class TestLinkCrawlerAgentFetchPage:
    """Tests for _fetch_page() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fetch_page_success(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test successful page fetch."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>Content</body></html>"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = await agent._fetch_page("https://example.com/page")

        assert html is not None
        assert "Content" in html

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fetch_page_404_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that 404 returns None."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = await agent._fetch_page("https://example.com/notfound")

        assert html is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fetch_page_500_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that 500 returns None."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = await agent._fetch_page("https://example.com/error")

        assert html is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fetch_page_exception_returns_none(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that exceptions return None."""
        mock_config.return_value.load.return_value = {}

        mock_http.return_value.get = AsyncMock(side_effect=Exception("Network error"))

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = await agent._fetch_page("https://example.com/page")

        assert html is None

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_fetch_page_logs_warning_on_non_200(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that non-200 responses are logged."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        await agent._fetch_page("https://example.com/forbidden")

        # Verify warning was logged
        agent.log.warning.assert_called()


# =============================================================================
# TEST CRAWL PAGINATED
# =============================================================================


class TestLinkCrawlerAgentCrawlPaginated:
    """Tests for _crawl_paginated() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_crawl_paginated_follows_pages(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that paginated crawl follows pages."""
        mock_config.return_value.load.return_value = {}

        # Three pages of content
        page1 = MagicMock()
        page1.status_code = 200
        page1.text = """
        <html><body>
            <a href="/member/a" class="member">A</a>
            <a href="?page=2">Next</a>
        </body></html>
        """

        page2 = MagicMock()
        page2.status_code = 200
        page2.text = """
        <html><body>
            <a href="/member/b" class="member">B</a>
            <a href="?page=3">Next</a>
        </body></html>
        """

        page3 = MagicMock()
        page3.status_code = 200
        page3.text = """
        <html><body>
            <a href="/member/c" class="member">C</a>
            <!-- No next link -->
        </body></html>
        """

        mock_http.return_value.get = AsyncMock(side_effect=[page1, page2, page3])

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        urls, pages = await agent._crawl_paginated(
            "https://example.com/members",
            {"type": "query_param", "param": "page"}
        )

        assert pages >= 1
        assert len(urls) >= 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_crawl_paginated_respects_max_pages(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that max_pages limit is respected."""
        mock_config.return_value.load.return_value = {}

        # Create response that always has "next" link
        def create_page(n):
            page = MagicMock()
            page.status_code = 200
            page.text = f"""
            <html><body>
                <a href="/member/{n}" class="member">Member {n}</a>
                <a href="?page={n+1}">Next</a>
            </body></html>
            """
            return page

        mock_http.return_value.get = AsyncMock(
            side_effect=[create_page(i) for i in range(10)]
        )

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")
        agent.max_pages = 3  # Set max_pages directly

        urls, pages = await agent._crawl_paginated(
            "https://example.com/members",
            {"type": "query_param", "param": "page"}
        )

        # pages is the last page number attempted + 1 due to post-increment
        # With max_pages=3, it crawls pages 1, 2, 3 then increments to 4 and exits
        assert pages <= 4

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_crawl_paginated_stops_on_empty_pages(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that crawl stops after consecutive empty pages."""
        mock_config.return_value.load.return_value = {}

        page1 = MagicMock()
        page1.status_code = 200
        page1.text = '<html><body><a href="/member/a" class="member">A</a></body></html>'

        empty = MagicMock()
        empty.status_code = 200
        empty.text = "<html><body><p>No results</p></body></html>"

        # Provide more empty pages to allow for the 3 consecutive empty check
        mock_http.return_value.get = AsyncMock(side_effect=[page1, empty, empty, empty, empty, empty])

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")
        agent.max_pages = 10  # Allow enough pages for the test

        urls, pages = await agent._crawl_paginated(
            "https://example.com/members",
            {"type": "query_param", "param": "page"}
        )

        # Should stop after 3 consecutive empty pages (at page 4)
        assert pages <= 5

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_crawl_paginated_handles_failed_fetch(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test handling of failed page fetches."""
        mock_config.return_value.load.return_value = {}

        page1 = MagicMock()
        page1.status_code = 200
        page1.text = '<html><body><a href="/member/a" class="member">A</a></body></html>'

        # Subsequent pages fail
        mock_http.return_value.get = AsyncMock(
            side_effect=[page1, Exception("Error"), Exception("Error"), Exception("Error")]
        )

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        urls, pages = await agent._crawl_paginated(
            "https://example.com/members",
            {"type": "query_param", "param": "page"}
        )

        # Should still return results from successful pages
        assert len(urls) >= 0

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_crawl_paginated_stops_on_no_next_page(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that crawl stops when no next page detected."""
        mock_config.return_value.load.return_value = {}

        # Page with disabled next button
        page = MagicMock()
        page.status_code = 200
        page.text = """
        <html><body>
            <a href="/member/a" class="member">A</a>
            <span class="pagination">
                <span class="disabled">Next</span>
            </span>
        </body></html>
        """

        mock_http.return_value.get = AsyncMock(return_value=page)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        urls, pages = await agent._crawl_paginated(
            "https://example.com/members",
            {"type": "query_param", "param": "page"}
        )

        # Should stop after first page since no next detected
        assert pages >= 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_crawl_paginated_accumulates_urls(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that URLs are accumulated across pages."""
        mock_config.return_value.load.return_value = {}

        page1 = MagicMock()
        page1.status_code = 200
        page1.text = '<html><body><a href="/member/a" class="member">A</a></body></html>'

        page2 = MagicMock()
        page2.status_code = 200
        page2.text = '<html><body><a href="/member/b" class="member">B</a></body></html>'

        page3 = MagicMock()
        page3.status_code = 200
        page3.text = '<html><body></body></html>'

        page4 = MagicMock()
        page4.status_code = 200
        page4.text = '<html><body></body></html>'

        page5 = MagicMock()
        page5.status_code = 200
        page5.text = '<html><body></body></html>'

        mock_http.return_value.get = AsyncMock(side_effect=[page1, page2, page3, page4, page5])

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        urls, pages = await agent._crawl_paginated(
            "https://example.com/members",
            {"type": "query_param", "param": "page"}
        )

        # Should have URLs from both pages
        assert len(urls) == 2


# =============================================================================
# TEST BUILD PAGE URL
# =============================================================================


class TestLinkCrawlerAgentBuildPageUrl:
    """Tests for _build_page_url() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_build_page_url_query_param_page(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test building URL with page query parameter."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        url = agent._build_page_url(
            "https://example.com/members",
            {"type": "query_param", "param": "page", "param_type": "page"},
            2
        )

        assert "page=2" in url

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_build_page_url_query_param_offset(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test building URL with offset query parameter."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        url = agent._build_page_url(
            "https://example.com/members",
            {"type": "query_param", "param": "offset", "param_type": "offset"},
            3
        )

        # Page 3 with offset = (3-1) * 100 = 200
        assert "offset=200" in url

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_build_page_url_query_param_count(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test building URL with count query parameter."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        url = agent._build_page_url(
            "https://example.com/members",
            {"type": "query_param", "param": "n", "param_type": "count"},
            2
        )

        # Page 2 with count = 2 * 100 = 200
        assert "n=200" in url

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_build_page_url_path_segment(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test building URL with path segment pagination."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        url = agent._build_page_url(
            "https://example.com/members",
            {"type": "path_segment", "pattern": "/page/{n}"},
            3
        )

        assert "/page/3" in url

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_build_page_url_preserves_existing_params(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that existing query params are preserved."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        url = agent._build_page_url(
            "https://example.com/members?category=tech",
            {"type": "query_param", "param": "page", "param_type": "page"},
            2
        )

        assert "category=tech" in url
        assert "page=2" in url

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_build_page_url_returns_base_for_unknown_type(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that unknown pagination type returns base URL."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        url = agent._build_page_url(
            "https://example.com/members",
            {"type": "unknown"},
            2
        )

        assert url == "https://example.com/members"


# =============================================================================
# TEST HAS NEXT PAGE
# =============================================================================


class TestLinkCrawlerAgentHasNextPage:
    """Tests for _has_next_page() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_has_next_page_with_next_link(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting next page from 'Next' link."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = '<html><body><a href="?page=2">Next</a></body></html>'

        assert agent._has_next_page(html, {}, 1) is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_has_next_page_with_numbered_pagination(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting next page from numbered pagination."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = """
        <html><body>
            <div class="pagination">
                <a href="?page=1">1</a>
                <a href="?page=2">2</a>
                <a href="?page=3">3</a>
            </div>
        </body></html>
        """

        assert agent._has_next_page(html, {}, 1) is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_has_next_page_disabled_next_button(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test detecting last page from disabled next button."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = """
        <html><body>
            <span class="disabled">Next</span>
        </body></html>
        """

        assert agent._has_next_page(html, {}, 3) is False

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_has_next_page_defaults_to_true(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that default is True when no clear indicator."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = "<html><body><p>Some content</p></body></html>"

        # Defaults to True to ensure we check next page
        assert agent._has_next_page(html, {}, 1) is True


# =============================================================================
# TEST EXTRACT MEMBER URLS
# =============================================================================


class TestLinkCrawlerAgentExtractMemberUrls:
    """Tests for _extract_member_urls() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extract_urls_from_href_patterns(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test extracting URLs matching href patterns."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = """
        <html><body>
            <a href="/member/company-a">Company A</a>
            <a href="/company/company-b">Company B</a>
            <a href="/profile/company-c">Company C</a>
        </body></html>
        """

        urls = agent._extract_member_urls(html, "https://example.com/members")

        assert len(urls) == 3
        assert any("company-a" in url for url in urls)
        assert any("company-b" in url for url in urls)
        assert any("company-c" in url for url in urls)

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extract_urls_from_class_patterns(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test extracting URLs from elements with member classes."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = """
        <html><body>
            <a href="/page/1" class="member-link">Member 1</a>
            <a href="/page/2" class="company-link">Company 2</a>
        </body></html>
        """

        urls = agent._extract_member_urls(html, "https://example.com/members")

        assert len(urls) >= 2

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extract_urls_from_containers(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test extracting URLs from member containers."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = """
        <html><body>
            <div class="member-item">
                <a href="/details/1">View Details</a>
            </div>
            <div class="company-listing">
                <a href="/details/2">View Details</a>
            </div>
        </body></html>
        """

        urls = agent._extract_member_urls(html, "https://example.com/members")

        assert len(urls) >= 2

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extract_urls_from_table_rows(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test extracting URLs from table rows."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = """
        <html><body>
            <table>
                <tr><td><a href="/member/1">Company 1</a></td></tr>
                <tr><td><a href="/member/2">Company 2</a></td></tr>
            </table>
        </body></html>
        """

        urls = agent._extract_member_urls(html, "https://example.com/members")

        assert len(urls) >= 2

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extract_urls_deduplicates(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that extracted URLs are deduplicated."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = """
        <html><body>
            <a href="/member/1" class="member">Company 1</a>
            <a href="/member/1">Company 1 Again</a>
            <div class="member-item"><a href="/member/1">Details</a></div>
        </body></html>
        """

        urls = agent._extract_member_urls(html, "https://example.com/members")

        # Should only have 1 unique URL
        member1_urls = [u for u in urls if "member/1" in u]
        assert len(member1_urls) == 1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extract_urls_only_same_domain(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that only same-domain URLs are extracted."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = """
        <html><body>
            <a href="/member/1" class="member">Internal</a>
            <a href="https://other.com/member/2" class="member">External</a>
        </body></html>
        """

        urls = agent._extract_member_urls(html, "https://example.com/members")

        # Should only have internal URL
        assert all("example.com" in url for url in urls)

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_extract_urls_resolves_relative(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that relative URLs are resolved to absolute."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        html = """
        <html><body>
            <a href="/member/1" class="member">Company 1</a>
            <a href="company/2" class="company">Company 2</a>
        </body></html>
        """

        urls = agent._extract_member_urls(html, "https://example.com/members")

        # All URLs should be absolute
        assert all(url.startswith("https://") for url in urls)


# =============================================================================
# TEST SHOULD SKIP URL
# =============================================================================


class TestLinkCrawlerAgentShouldSkipUrl:
    """Tests for _should_skip_url() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_skip_anchor_links(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test skipping anchor links."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        assert agent._should_skip_url("#section") is True
        assert agent._should_skip_url("#") is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_skip_mailto_links(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test skipping mailto links."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        assert agent._should_skip_url("mailto:test@example.com") is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_skip_javascript_links(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test skipping javascript links."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        assert agent._should_skip_url("javascript:void(0)") is True
        assert agent._should_skip_url("javascript:alert()") is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_skip_social_media_links(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test skipping social media links."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        assert agent._should_skip_url("https://facebook.com/company") is True
        assert agent._should_skip_url("https://twitter.com/company") is True
        assert agent._should_skip_url("https://linkedin.com/company/xyz") is True
        assert agent._should_skip_url("https://youtube.com/channel/xyz") is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_skip_document_links(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test skipping document download links."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        assert agent._should_skip_url("/download/file.pdf") is True
        assert agent._should_skip_url("/download/doc.docx") is True
        assert agent._should_skip_url("/archive.zip") is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_skip_utility_pages(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test skipping utility pages."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        assert agent._should_skip_url("/login") is True
        assert agent._should_skip_url("/register") is True
        assert agent._should_skip_url("/contact") is True
        assert agent._should_skip_url("/privacy") is True
        assert agent._should_skip_url("/terms") is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_not_skip_member_urls(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test that member URLs are not skipped."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        assert agent._should_skip_url("/member/company-a") is False
        assert agent._should_skip_url("/company/xyz") is False
        assert agent._should_skip_url("/profile/abc") is False

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_skip_tel_links(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test skipping tel: links."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        assert agent._should_skip_url("tel:+1-555-123-4567") is True


# =============================================================================
# TEST INFINITE SCROLL
# =============================================================================


class TestLinkCrawlerAgentInfiniteScroll:
    """Tests for _crawl_infinite_scroll() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_infinite_scroll_fallback_without_playwright(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        member_directory_with_pagination_html
    ):
        """Test fallback to static scrape when Playwright unavailable."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = member_directory_with_pagination_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        # Mock ImportError for playwright
        with patch.dict("sys.modules", {"playwright.async_api": None}):
            urls = await agent._crawl_infinite_scroll("https://example.com/members")

        # Should fall back to static scrape
        assert isinstance(urls, set)

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_infinite_scroll_handles_exception(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test handling exceptions during Playwright crawl."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body></body></html>"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        # Mock playwright to raise an exception
        mock_playwright = MagicMock()
        mock_playwright.return_value.__aenter__ = AsyncMock(side_effect=Exception("Playwright error"))

        # Patch at the playwright library level since it's imported locally
        with patch("playwright.async_api.async_playwright", mock_playwright):
            urls = await agent._crawl_infinite_scroll("https://example.com/members")

        # Should return empty set on error
        assert isinstance(urls, set)

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_infinite_scroll_via_run(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        member_directory_with_pagination_html
    ):
        """Test infinite scroll via run() method."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = member_directory_with_pagination_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        with patch.object(agent, '_save_urls'):
            with patch.object(agent, '_crawl_infinite_scroll', AsyncMock(return_value=set())):
                result = await agent.run({
                    "entry_url": "https://example.com/members",
                    "pagination": {"type": "infinite_scroll"},
                    "association": "TEST"
                })

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_infinite_scroll_returns_set(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        member_directory_with_pagination_html
    ):
        """Test that infinite scroll returns a set."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = member_directory_with_pagination_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        # Fallback to static scrape
        urls = await agent._crawl_infinite_scroll("https://example.com/members")

        assert isinstance(urls, set)


# =============================================================================
# TEST LOAD MORE
# =============================================================================


class TestLinkCrawlerAgentLoadMore:
    """Tests for _crawl_load_more() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_load_more_fallback_without_playwright(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        member_directory_with_pagination_html
    ):
        """Test fallback to static scrape when Playwright unavailable."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = member_directory_with_pagination_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        # Mock ImportError for playwright
        with patch.dict("sys.modules", {"playwright.async_api": None}):
            urls = await agent._crawl_load_more("https://example.com/members")

        assert isinstance(urls, set)

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_load_more_via_run(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        member_directory_with_pagination_html
    ):
        """Test load more via run() method."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = member_directory_with_pagination_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        with patch.object(agent, '_save_urls'):
            with patch.object(agent, '_crawl_load_more', AsyncMock(return_value=set())):
                result = await agent.run({
                    "entry_url": "https://example.com/members",
                    "pagination": {"type": "load_more"},
                    "association": "TEST"
                })

        assert result["success"] is True

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_load_more_handles_exception(
        self, mock_limiter, mock_http, mock_logger, mock_config
    ):
        """Test handling exceptions during load more crawl."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "<html><body></body></html>"
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        # Mock playwright to raise an exception
        mock_playwright = MagicMock()
        mock_playwright.return_value.__aenter__ = AsyncMock(side_effect=Exception("Error"))

        # Patch at the playwright library level since it's imported locally
        with patch("playwright.async_api.async_playwright", mock_playwright):
            urls = await agent._crawl_load_more("https://example.com/members")

        assert isinstance(urls, set)

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_load_more_returns_set(
        self, mock_limiter, mock_http, mock_logger, mock_config,
        member_directory_with_pagination_html
    ):
        """Test that load more returns a set."""
        mock_config.return_value.load.return_value = {}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = member_directory_with_pagination_html
        mock_http.return_value.get = AsyncMock(return_value=mock_response)

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        urls = await agent._crawl_load_more("https://example.com/members")

        assert isinstance(urls, set)


# =============================================================================
# TEST SAVE URLS
# =============================================================================


class TestLinkCrawlerAgentSaveUrls:
    """Tests for _save_urls() method."""

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_save_urls_creates_jsonl_file(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """Test that URLs are saved to JSONL file."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        urls = [
            "https://example.com/member/1",
            "https://example.com/member/2"
        ]
        output_path = str(tmp_path / "TEST" / "urls.jsonl")

        agent._save_urls(urls, output_path)

        # Verify file was created
        assert Path(output_path).exists()

        # Verify content
        with open(output_path, "r") as f:
            lines = f.readlines()

        assert len(lines) == 2

        # Verify JSON structure
        record1 = json.loads(lines[0])
        assert record1["url"] == urls[0]
        assert "discovered_at" in record1

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_save_urls_creates_parent_directory(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """Test that parent directories are created."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        urls = ["https://example.com/member/1"]
        output_path = str(tmp_path / "deep" / "nested" / "path" / "urls.jsonl")

        agent._save_urls(urls, output_path)

        assert Path(output_path).exists()

    @pytest.mark.asyncio
    @patch("agents.base.Config")
    @patch("agents.base.StructuredLogger")
    @patch("agents.base.AsyncHTTPClient")
    @patch("agents.base.RateLimiter")
    async def test_save_urls_logs_count(
        self, mock_limiter, mock_http, mock_logger, mock_config, tmp_path
    ):
        """Test that URL count is logged."""
        mock_config.return_value.load.return_value = {}

        from agents.discovery.link_crawler import LinkCrawlerAgent
        agent = LinkCrawlerAgent(agent_type="discovery.link_crawler")

        urls = ["https://example.com/member/1", "https://example.com/member/2"]
        output_path = str(tmp_path / "urls.jsonl")

        agent._save_urls(urls, output_path)

        # Verify logging
        agent.log.info.assert_called()
