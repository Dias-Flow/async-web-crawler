"""
async_crawler/test_day3_day4.py

Tests for Day 3 (CrawlerQueue, SemaphoreManager) and Day 4 (RateLimiter, RobotsParser).
All tests are offline — no real HTTP requests.

Run with:
    pytest test_day3_day4.py -v
"""

import asyncio
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from queue_manager import CrawlerQueue, SemaphoreManager
from rate_limiter import RateLimiter, RobotsParser

pytestmark = pytest.mark.asyncio


# ===========================================================================
# CrawlerQueue
# ===========================================================================
class TestCrawlerQueue:

    async def test_add_new_url_returns_true(self):
        """First add of a URL should succeed."""
        q = CrawlerQueue()
        added = await q.add_url("https://example.com")
        assert added is True

    async def test_add_duplicate_returns_false(self):
        """Adding the same URL twice should return False the second time."""
        q = CrawlerQueue()
        await q.add_url("https://example.com")
        added_again = await q.add_url("https://example.com")
        assert added_again is False

    async def test_get_next_returns_url(self):
        """get_next() returns str|None per Day-3 spec."""
        q = CrawlerQueue()
        await q.add_url("https://example.com", depth=2)
        url = await q.get_next()
        assert url == "https://example.com"

    async def test_get_depth_returns_correct_depth(self):
        """get_depth() returns the depth stored when the URL was added."""
        q = CrawlerQueue()
        await q.add_url("https://example.com", depth=2)
        await q.get_next()
        assert q.get_depth("https://example.com") == 2

    async def test_get_next_empty_returns_none(self):
        """get_next() on an empty queue should return None, not raise."""
        q = CrawlerQueue()
        item = await q.get_next()
        assert item is None

    async def test_priority_order(self):
        """Lower priority number should come out first."""
        q = CrawlerQueue()
        await q.add_url("https://example.com/low", priority=10)
        await q.add_url("https://example.com/high", priority=1)
        first = await q.get_next()
        assert first == "https://example.com/high"

    async def test_mark_processed_updates_state(self):
        """After mark_processed the URL should appear in visited stats."""
        q = CrawlerQueue()
        await q.add_url("https://example.com")
        await q.get_next()
        q.mark_processed("https://example.com")
        stats = q.get_stats()
        assert stats["visited"] == 1

    async def test_mark_failed_updates_state(self):
        """After mark_failed the URL should appear in failed stats."""
        q = CrawlerQueue()
        await q.add_url("https://example.com")
        await q.get_next()
        q.mark_failed("https://example.com", "timeout")
        stats = q.get_stats()
        assert stats["failed"] == 1

    async def test_stats_total_seen(self):
        """get_stats total_seen should equal number of unique adds."""
        q = CrawlerQueue()
        for i in range(5):
            await q.add_url(f"https://example.com/{i}")
        stats = q.get_stats()
        assert stats["total_seen"] == 5

    async def test_is_known_after_add(self):
        """is_known() must be True right after adding a URL."""
        q = CrawlerQueue()
        await q.add_url("https://example.com/page")
        assert q.is_known("https://example.com/page") is True

    async def test_is_known_false_for_unseen(self):
        q = CrawlerQueue()
        assert q.is_known("https://never-added.com") is False

    async def test_no_duplicates_under_concurrency(self):
        """Concurrent adds of the same URL should result in exactly one entry."""
        q = CrawlerQueue()
        url = "https://example.com/concurrent"
        results = await asyncio.gather(*[q.add_url(url) for _ in range(20)])
        assert sum(results) == 1        # only one True
        assert q.get_stats()["total_seen"] == 1


# ===========================================================================
# SemaphoreManager
# ===========================================================================
class TestSemaphoreManager:

    async def test_domain_context_acquires_and_releases(self):
        """Should be usable as an async context manager without error."""
        sm = SemaphoreManager(global_limit=5, per_domain_limit=2)
        ctx = await sm.domain_context("https://example.com/page")
        async with ctx:
            pass   # no exception = success

    async def test_per_domain_limit_enforced(self):
        """
        With per_domain_limit=2, at most 2 coroutines should be inside
        the context simultaneously for the same domain.
        """
        sm = SemaphoreManager(global_limit=10, per_domain_limit=2)
        active = 0
        peak = 0
        lock = asyncio.Lock()

        async def simulate(_):
            nonlocal active, peak
            ctx = await sm.domain_context("https://example.com/x")
            async with ctx:
                async with lock:
                    active += 1
                    peak = max(peak, active)
                await asyncio.sleep(0.05)
                async with lock:
                    active -= 1

        await asyncio.gather(*[simulate(i) for i in range(10)])
        assert peak <= 2

    async def test_different_domains_independent(self):
        """Two different domains should not block each other."""
        sm = SemaphoreManager(global_limit=10, per_domain_limit=1)
        results = []

        async def fetch(url):
            ctx = await sm.domain_context(url)
            async with ctx:
                await asyncio.sleep(0.05)
                results.append(url)

        t0 = time.perf_counter()
        await asyncio.gather(
            fetch("https://alpha.com/"),
            fetch("https://beta.com/"),
        )
        elapsed = time.perf_counter() - t0
        # Both should run near-simultaneously, not sequentially (< 0.15s total)
        assert elapsed < 0.15
        assert len(results) == 2


# ===========================================================================
# RateLimiter
# ===========================================================================
class TestRateLimiter:

    async def test_first_request_no_wait(self):
        """The very first request to a domain should not wait."""
        rl = RateLimiter(requests_per_second=1.0, jitter=0.0)
        waited = await rl.acquire("https://example.com/")
        assert waited == 0.0

    async def test_second_request_waits(self):
        """The second request to the same domain should wait ~1 second."""
        rl = RateLimiter(requests_per_second=1.0, min_delay=0.0, jitter=0.0)
        await rl.acquire("https://example.com/")
        t0 = time.monotonic()
        waited = await rl.acquire("https://example.com/")
        elapsed = time.monotonic() - t0
        assert waited > 0
        assert elapsed >= 0.9   # allow small timing variance

    async def test_different_domains_independent(self):
        """Two different domains should not block each other."""
        rl = RateLimiter(requests_per_second=1.0, per_domain=True, jitter=0.0)
        await rl.acquire("https://alpha.com/")
        # Immediately requesting a different domain should not wait
        t0 = time.monotonic()
        await rl.acquire("https://beta.com/")
        elapsed = time.monotonic() - t0
        assert elapsed < 0.1

    async def test_global_mode_blocks_different_domains(self):
        """When per_domain=False all URLs share one bucket."""
        rl = RateLimiter(requests_per_second=2.0, per_domain=False, jitter=0.0)
        await rl.acquire("https://alpha.com/")
        t0 = time.monotonic()
        # Different domain but same global bucket — should still wait
        waited = await rl.acquire("https://beta.com/")
        assert waited > 0

    async def test_min_delay_respected(self):
        """min_delay overrides a high requests_per_second."""
        rl = RateLimiter(requests_per_second=100.0, min_delay=0.3, jitter=0.0)
        await rl.acquire("https://example.com/")
        t0 = time.monotonic()
        await rl.acquire("https://example.com/")
        elapsed = time.monotonic() - t0
        assert elapsed >= 0.28   # allow small timing tolerance

    async def test_stats_increment(self):
        """total_waits should increase after each wait."""
        rl = RateLimiter(requests_per_second=100.0, min_delay=0.05, jitter=0.0)
        await rl.acquire("https://example.com/")
        await rl.acquire("https://example.com/")
        stats = rl.get_stats()
        assert stats["total_waits"] >= 1


# ===========================================================================
# RobotsParser
# ===========================================================================
class TestRobotsParser:

    def _make_session_mock(self, robots_text: str, status: int = 200):
        """Build a fake aiohttp session that returns robots_text."""
        mock_resp = AsyncMock()
        mock_resp.status = status
        mock_resp.text = AsyncMock(return_value=robots_text)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)
        mock_session = MagicMock()
        mock_session.get = MagicMock(return_value=mock_resp)
        return mock_session

    async def test_allow_all_when_no_robots(self):
        """A 404 robots.txt should allow everything."""
        rp = RobotsParser()
        session = self._make_session_mock("", status=404)
        rfp = await rp.fetch_robots(session, "https://example.com")
        assert rp.can_fetch(rfp, "https://example.com/anything") is True

    async def test_disallow_respected(self):
        """Disallow: /private should block that path."""
        robots_txt = "User-agent: *\nDisallow: /private\n"
        rp = RobotsParser(user_agent="*")
        session = self._make_session_mock(robots_txt)
        rfp = await rp.fetch_robots(session, "https://example.com")
        assert rp.can_fetch(rfp, "https://example.com/private/secret") is False
        assert rp.can_fetch(rfp, "https://example.com/public") is True

    async def test_crawl_delay_parsed(self):
        """Crawl-delay directive should be returned by get_crawl_delay()."""
        robots_txt = "User-agent: *\nCrawl-delay: 5\n"
        rp = RobotsParser(user_agent="*")
        session = self._make_session_mock(robots_txt)
        rfp = await rp.fetch_robots(session, "https://example.com")
        delay = rp.get_crawl_delay(rfp)
        assert delay == 5.0

    async def test_cache_prevents_second_fetch(self):
        """Second call for same domain must not hit the network again."""
        robots_txt = "User-agent: *\nDisallow:\n"
        rp = RobotsParser()
        session = self._make_session_mock(robots_txt)
        await rp.fetch_robots(session, "https://example.com")
        call_count_after_first = session.get.call_count
        await rp.fetch_robots(session, "https://example.com/other/page")
        # session.get should NOT have been called again
        assert session.get.call_count == call_count_after_first

    async def test_blocked_count_increments(self):
        """Blocked URLs should be counted in stats."""
        robots_txt = "User-agent: *\nDisallow: /\n"
        rp = RobotsParser()
        session = self._make_session_mock(robots_txt)
        rfp = await rp.fetch_robots(session, "https://example.com")
        rp.can_fetch(rfp, "https://example.com/page1")
        rp.can_fetch(rfp, "https://example.com/page2")
        assert rp.get_stats()["total_blocked"] == 2


# ===========================================================================
# Task tracking tests (Fix 1)
# ===========================================================================
class TestCrawlerQueueTaskTracking:

    async def test_crawl_tasks_set_exists_after_init(self):
        """_crawl_tasks set must be created by _init_crawl_components."""
        from crawler import AsyncCrawler
        async with AsyncCrawler() as c:
            c._init_crawl_components()
            assert hasattr(c, "_crawl_tasks")
            assert isinstance(c._crawl_tasks, set)
            assert len(c._crawl_tasks) == 0


# ===========================================================================
# robots.txt block counted in failed_urls (Fix 2) — queue_manager level
# ===========================================================================
class TestRobotsBlockInFailedUrls:

    async def test_mark_failed_stores_error(self):
        """mark_failed should store the error message in URLRecord."""
        q = CrawlerQueue()
        await q.add_url("https://example.com/private")
        await q.get_next()
        q.mark_failed("https://example.com/private", "blocked by robots.txt")
        failed = q.get_failed()
        assert "https://example.com/private" in failed
        assert failed["https://example.com/private"] == "blocked by robots.txt"
