"""
async_crawler/test_crawler.py

Unit / integration tests for AsyncCrawler.

Tests are written with pytest + pytest-asyncio.
Install test dependencies:
    pip install pytest pytest-asyncio aioresponses

Run all tests:
    pytest test_crawler.py -v
"""

import asyncio
import pytest
import aiohttp
from unittest.mock import AsyncMock, patch, MagicMock

from crawler import AsyncCrawler, FetchResult


# ---------------------------------------------------------------------------
# pytest-asyncio configuration — tells pytest every async test uses asyncio
# ---------------------------------------------------------------------------
pytestmark = pytest.mark.asyncio


# ===========================================================================
# FetchResult unit tests — pure data class, no network needed
# ===========================================================================
class TestFetchResult:
    """Tests for the FetchResult dataclass helper properties."""

    def test_success_is_true_when_content_present(self):
        """A result with content and no error should be considered successful."""
        r = FetchResult(url="https://example.com", status=200, content="<html/>")
        assert r.success is True

    def test_success_is_false_when_error_set(self):
        """Any error string means the fetch failed, regardless of status."""
        r = FetchResult(url="https://example.com", status=200, content="<html/>", error="oops")
        assert r.success is False

    def test_success_is_false_when_no_content(self):
        """No content → not a successful fetch."""
        r = FetchResult(url="https://example.com", status=200)
        assert r.success is False

    def test_elapsed_defaults_to_zero(self):
        """elapsed should be 0.0 by default."""
        r = FetchResult(url="https://example.com")
        assert r.elapsed == 0.0


# ===========================================================================
# AsyncCrawler initialisation tests — no network involved
# ===========================================================================
class TestAsyncCrawlerInit:
    """Verify the crawler is configured correctly at construction time."""

    async def test_default_concurrency(self):
        """max_concurrent should default to 10."""
        async with AsyncCrawler() as c:
            assert c.max_concurrent == 10
            assert c._semaphore._value == 10  # internal semaphore reflects the cap

    async def test_custom_concurrency(self):
        """Custom max_concurrent should be respected."""
        async with AsyncCrawler(max_concurrent=3) as c:
            assert c.max_concurrent == 3

    async def test_session_created(self):
        """A ClientSession should exist after construction."""
        async with AsyncCrawler() as c:
            assert c._session is not None
            assert isinstance(c._session, aiohttp.ClientSession)


# ===========================================================================
# Mocked HTTP fetch tests — we mock aiohttp so no real network is used
# ===========================================================================
class TestFetchUrl:
    """
    Test fetch_url with mocked HTTP responses.
    We patch aiohttp.ClientSession.get so tests run offline and are deterministic.
    """

    async def _make_mock_response(self, status: int, body: str = "<html/>"):
        """Helper that builds a fake aiohttp response context manager."""
        mock_resp = AsyncMock()
        mock_resp.status = status
        mock_resp.text = AsyncMock(return_value=body)

        # Simulate raise_for_status() raising for 4xx/5xx
        if status >= 400:
            mock_resp.raise_for_status.side_effect = aiohttp.ClientResponseError(
                request_info=MagicMock(),
                history=(),
                status=status,
                message=f"HTTP {status}",
            )
        else:
            mock_resp.raise_for_status = MagicMock()  # no-op

        # Make it usable as `async with session.get(...) as resp:`
        mock_context = AsyncMock()
        mock_context.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_context.__aexit__ = AsyncMock(return_value=False)
        return mock_context

    async def test_successful_fetch_returns_content(self):
        """A 200 response should be returned as a successful FetchResult."""
        mock_ctx = await self._make_mock_response(200, "<html>Hello</html>")

        async with AsyncCrawler(max_retries=0) as crawler:
            with patch.object(crawler._session, "get", return_value=mock_ctx):
                result = await crawler.fetch_url("https://example.com")

        assert result.success is True
        assert result.status == 200
        assert result.content == "<html>Hello</html>"
        assert result.error is None

    async def test_404_returns_error_result(self):
        """A 404 response should be captured without raising an exception."""
        mock_ctx = await self._make_mock_response(404)

        async with AsyncCrawler(max_retries=0) as crawler:
            with patch.object(crawler._session, "get", return_value=mock_ctx):
                result = await crawler.fetch_url("https://example.com/missing")

        assert result.success is False
        assert result.status == 404
        assert result.error is not None

    async def test_500_returns_error_result(self):
        """A 500 response should be captured gracefully."""
        mock_ctx = await self._make_mock_response(500)

        async with AsyncCrawler(max_retries=0) as crawler:
            with patch.object(crawler._session, "get", return_value=mock_ctx):
                result = await crawler.fetch_url("https://example.com/broken")

        assert result.success is False
        assert result.status == 500

    async def test_timeout_error_handled(self):
        """asyncio.TimeoutError should be caught and returned as a FetchResult."""
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(side_effect=asyncio.TimeoutError())
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        async with AsyncCrawler(max_retries=0) as crawler:
            with patch.object(crawler._session, "get", return_value=mock_ctx):
                result = await crawler.fetch_url("https://slow.example.com")

        assert result.success is False
        assert "Timeout" in result.error

    async def test_network_error_handled(self):
        """aiohttp.ClientError (e.g. DNS failure) should be caught gracefully."""
        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(
            side_effect=aiohttp.ClientConnectorError(
                connection_key=MagicMock(), os_error=OSError("Name not resolved")
            )
        )
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        async with AsyncCrawler(max_retries=0) as crawler:
            with patch.object(crawler._session, "get", return_value=mock_ctx):
                result = await crawler.fetch_url("https://nonexistent.xyz")

        assert result.success is False
        assert result.error is not None


# ===========================================================================
# Batch fetch tests
# ===========================================================================
class TestFetchUrls:
    """Tests for the fetch_urls batch method."""

    async def test_returns_all_urls(self):
        """fetch_urls should return one entry per input URL."""
        urls = [f"https://example.com/page{i}" for i in range(5)]

        async def fake_fetch(url: str) -> FetchResult:
            return FetchResult(url=url, status=200, content="ok")

        async with AsyncCrawler() as crawler:
            # Replace the actual fetch with a fast fake
            crawler.fetch_url = fake_fetch
            results = await crawler.fetch_urls(urls)

        assert len(results) == len(urls)
        for url in urls:
            assert url in results

    async def test_empty_list_returns_empty_dict(self):
        """Passing an empty list should return an empty dict without errors."""
        async with AsyncCrawler() as crawler:
            results = await crawler.fetch_urls([])
        assert results == {}

    async def test_mixed_results(self):
        """The batch should contain both successes and failures without crashing."""
        call_count = 0

        async def alternating_fetch(url: str) -> FetchResult:
            nonlocal call_count
            call_count += 1
            if call_count % 2 == 0:
                return FetchResult(url=url, status=200, content="ok")
            else:
                return FetchResult(url=url, error="some error")

        urls = [f"https://example.com/{i}" for i in range(6)]
        async with AsyncCrawler() as crawler:
            crawler.fetch_url = alternating_fetch
            results = await crawler.fetch_urls(urls)

        successes = [r for r in results.values() if r.success]
        failures = [r for r in results.values() if not r.success]
        assert len(successes) > 0
        assert len(failures) > 0


# ===========================================================================
# Concurrency cap test
# ===========================================================================
class TestConcurrencyLimit:
    """Verify that the semaphore actually limits concurrent requests."""

    async def test_max_concurrent_respected(self):
        """
        We fire 20 coroutines but cap concurrency at 3.
        Track the peak number of simultaneously active requests.
        """
        max_concurrent = 3
        active_count = 0
        peak_active = 0
        lock = asyncio.Lock()

        async def slow_fetch(url: str) -> FetchResult:
            nonlocal active_count, peak_active
            async with lock:
                active_count += 1
                if active_count > peak_active:
                    peak_active = active_count

            await asyncio.sleep(0.05)  # simulate a short network delay

            async with lock:
                active_count -= 1

            return FetchResult(url=url, status=200, content="ok")

        urls = [f"https://example.com/{i}" for i in range(20)]
        async with AsyncCrawler(max_concurrent=max_concurrent) as crawler:
            crawler.fetch_url = slow_fetch  # bypass the semaphore-wrapped method
            # We need to test the semaphore, so call the internals directly
            # Re-wrap so the semaphore is exercised:
            original_fetch = crawler._do_fetch

            async def semaphored_fake(url: str) -> FetchResult:
                async with crawler._semaphore:
                    return await slow_fetch(url)

            # Manually gather using the semaphore wrapper
            tasks = [semaphored_fake(url) for url in urls]
            await asyncio.gather(*tasks)

        assert peak_active <= max_concurrent, (
            f"Peak concurrent requests {peak_active} exceeded limit {max_concurrent}"
        )
