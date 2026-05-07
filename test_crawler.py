"""
async_crawler/test_crawler.py  (Day 1 tests - updated to match str API)

fetch_url()  -> str     ("" on failure)
fetch_urls() -> dict[str, str]

Run with:  pytest test_crawler.py -v
"""

import asyncio
import pytest
import aiohttp
from unittest.mock import AsyncMock, patch, MagicMock

from crawler import AsyncCrawler, FetchResult   # FetchResult still importable (internal use)

pytestmark = pytest.mark.asyncio


# ===========================================================================
# FetchResult unit tests (internal dataclass - still testable)
# ===========================================================================
class TestFetchResult:
    def test_success_true_when_content(self):
        r = FetchResult(url="https://x.com", status=200, content="<html/>")
        assert r.success is True

    def test_success_false_when_error(self):
        r = FetchResult(url="https://x.com", status=200, content="<html/>", error="oops")
        assert r.success is False

    def test_success_false_when_no_content(self):
        r = FetchResult(url="https://x.com", status=200)
        assert r.success is False

    def test_elapsed_defaults_zero(self):
        r = FetchResult(url="https://x.com")
        assert r.elapsed == 0.0


# ===========================================================================
# AsyncCrawler init tests
# ===========================================================================
class TestAsyncCrawlerInit:
    async def test_default_concurrency(self):
        async with AsyncCrawler() as c:
            assert c.max_concurrent == 10

    async def test_custom_concurrency(self):
        async with AsyncCrawler(max_concurrent=3) as c:
            assert c.max_concurrent == 3

    async def test_session_created(self):
        async with AsyncCrawler() as c:
            assert isinstance(c._session, aiohttp.ClientSession)

    async def test_storage_none_by_default(self):
        async with AsyncCrawler() as c:
            assert c.storage is None

    async def test_storage_injected(self):
        """Day 6: AsyncCrawler accepts a storage parameter."""
        class FakeStorage:
            async def save(self, d): pass
            async def close(self): pass
        s = FakeStorage()
        async with AsyncCrawler(storage=s) as c:
            assert c.storage is s


# ===========================================================================
# fetch_url tests — returns str per Day-1 spec
# ===========================================================================
class TestFetchUrl:

    def _mock_ctx(self, status: int, body: str = "<html/>"):
        """Build a fake aiohttp response context manager."""
        mock_resp = AsyncMock()
        mock_resp.status = status
        mock_resp.text = AsyncMock(return_value=body)
        # headers MUST be a real dict — AsyncMock would make .get() return a coroutine
        mock_resp.headers = {"Content-Type": "text/html"}
        # raise_for_status MUST be a sync MagicMock (aiohttp raises synchronously)
        if status >= 400:
            mock_resp.raise_for_status = MagicMock(
                side_effect=aiohttp.ClientResponseError(
                    request_info=MagicMock(), history=(), status=status, message=f"HTTP {status}"))
        else:
            mock_resp.raise_for_status = MagicMock()   # no-op for 2xx
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        ctx.__aexit__  = AsyncMock(return_value=False)
        return ctx

    async def test_success_returns_html_string(self):
        """200 response: fetch_url() returns the HTML body as str."""
        ctx = self._mock_ctx(200, "<html>Hello</html>")
        async with AsyncCrawler(max_retries=0) as crawler:
            with patch.object(crawler._session, "get", return_value=ctx):
                result = await crawler.fetch_url("https://example.com")
        assert isinstance(result, str)
        assert result == "<html>Hello</html>"

    async def test_404_returns_empty_string(self):
        """4xx error: fetch_url() returns empty string, does not raise."""
        ctx = self._mock_ctx(404)
        async with AsyncCrawler(max_retries=0) as crawler:
            with patch.object(crawler._session, "get", return_value=ctx):
                result = await crawler.fetch_url("https://example.com/missing")
        assert isinstance(result, str)
        assert result == ""

    async def test_500_returns_empty_string(self):
        ctx = self._mock_ctx(500)
        async with AsyncCrawler(max_retries=0) as crawler:
            with patch.object(crawler._session, "get", return_value=ctx):
                result = await crawler.fetch_url("https://example.com/broken")
        assert result == ""

    async def test_timeout_returns_empty_string(self):
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(side_effect=asyncio.TimeoutError())
        ctx.__aexit__  = AsyncMock(return_value=False)
        async with AsyncCrawler(max_retries=0) as crawler:
            with patch.object(crawler._session, "get", return_value=ctx):
                result = await crawler.fetch_url("https://slow.example.com")
        assert result == ""

    async def test_network_error_returns_empty_string(self):
        ctx = AsyncMock()
        ctx.__aenter__ = AsyncMock(
            side_effect=aiohttp.ClientConnectorError(
                connection_key=MagicMock(), os_error=OSError("DNS fail")))
        ctx.__aexit__ = AsyncMock(return_value=False)
        async with AsyncCrawler(max_retries=0) as crawler:
            with patch.object(crawler._session, "get", return_value=ctx):
                result = await crawler.fetch_url("https://nonexistent.xyz")
        assert result == ""


# ===========================================================================
# fetch_urls tests — returns dict[str, str] per spec
# ===========================================================================
class TestFetchUrls:

    async def test_returns_dict_of_str(self):
        """fetch_urls returns {url: html_string} for all inputs."""
        urls = [f"https://example.com/page{i}" for i in range(5)]

        async def fake_fetch(url: str) -> str:
            return f"<html>{url}</html>"

        async with AsyncCrawler() as crawler:
            crawler.fetch_url = fake_fetch
            results = await crawler.fetch_urls(urls)

        assert isinstance(results, dict)
        assert len(results) == 5
        for url in urls:
            assert url in results
            assert isinstance(results[url], str)

    async def test_empty_list_returns_empty_dict(self):
        async with AsyncCrawler() as crawler:
            results = await crawler.fetch_urls([])
        assert results == {}

    async def test_failed_urls_get_empty_string(self):
        """Failed fetches produce "" values in the dict, not missing keys."""
        urls = ["https://ok.com", "https://bad.com"]

        async def fake_fetch(url: str) -> str:
            return "<html/>" if "ok" in url else ""

        async with AsyncCrawler() as crawler:
            crawler.fetch_url = fake_fetch
            results = await crawler.fetch_urls(urls)

        assert "https://ok.com"  in results
        assert "https://bad.com" in results
        assert results["https://bad.com"] == ""


# ===========================================================================
# Concurrency cap test
# ===========================================================================
class TestConcurrencyLimit:

    async def test_max_concurrent_respected(self):
        """Semaphore caps peak concurrent requests at max_concurrent."""
        max_concurrent = 3
        active_count = 0
        peak_active  = 0
        lock = asyncio.Lock()

        async def semaphored_fake(url: str) -> str:
            nonlocal active_count, peak_active
            async with crawler._semaphore:
                async with lock:
                    active_count += 1
                    peak_active = max(peak_active, active_count)
                await asyncio.sleep(0.05)
                async with lock:
                    active_count -= 1
            return "<html/>"

        urls = [f"https://example.com/{i}" for i in range(20)]
        async with AsyncCrawler(max_concurrent=max_concurrent) as crawler:
            tasks = [semaphored_fake(url) for url in urls]
            await asyncio.gather(*tasks)

        assert peak_active <= max_concurrent
