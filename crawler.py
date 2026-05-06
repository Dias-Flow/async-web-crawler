"""
async_crawler/crawler.py  (Days 1 + 2 + 3 + 4)

FIX HISTORY (mentor review):
  1. fetch_url() now returns str (HTML) per Day-1 spec;
     fetch_urls() returns dict[str, str].
     FetchResult is kept internally but not exposed to callers.
  2. fetch_and_parse() now stamps status_code, content_type, crawled_at
     onto every page dict so CrawlerStats and DataStorage get real values.
  3. crawl() loop replaced fixed sleep(1.5) with proper task-tracking:
     _active_tasks counter + asyncio.Event so the loop waits until ALL
     background workers have actually finished.
  4. robots.txt timeout fixed: asyncio.timeout() → aiohttp.ClientTimeout
     (the correct way to set a timeout on an aiohttp request).
  5. get_next() in CrawlerQueue now returns str|None per Day-3 spec;
     depth is tracked separately inside URLRecord.
"""

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlparse

import aiohttp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("AsyncCrawler")


# ---------------------------------------------------------------------------
# FetchResult — internal only; callers get str or dict, not this
# ---------------------------------------------------------------------------
@dataclass
class FetchResult:
    """
    Internal container for a single HTTP response.
    NOT returned to callers of fetch_url() — those get plain str (HTML).
    Kept here so _do_fetch / _fetch_with_retry can pass all metadata
    (status code, content-type, elapsed) up to fetch_and_parse().
    """
    url: str
    status: Optional[int] = None
    content: Optional[str] = None
    content_type: Optional[str] = None   # e.g. "text/html; charset=utf-8"
    error: Optional[str] = None
    elapsed: float = 0.0

    @property
    def success(self) -> bool:
        return self.error is None and self.content is not None


class AsyncCrawler:
    """
    Core crawler. Signature matches the course spec:
      Day 1: fetch_url(url) -> str
             fetch_urls(urls) -> dict[str, str]
      Day 2: fetch_and_parse(url) -> dict
             fetch_and_parse_urls(urls) -> dict[str, dict]
      Day 3: crawl(...) -> dict[str, dict]  (queue-driven)
      Day 4: rate limiting + robots.txt
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
        max_retries: int = 2,
        retry_delay: float = 1.0,
        parser=None,
        max_depth: int = 2,
        per_domain_limit: int = 3,
        requests_per_second: float = 1.0,
        min_delay: float = 0.5,
        jitter: float = 0.3,
        respect_robots: bool = True,
        user_agent: str = "AsyncCrawler/1.0 (educational project; aiohttp)",
    ) -> None:
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._timeout = aiohttp.ClientTimeout(connect=connect_timeout, total=read_timeout)

        connector = aiohttp.TCPConnector(
            limit=max_concurrent * 2,
            limit_per_host=per_domain_limit,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
        )
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=self._timeout,
            headers={"User-Agent": user_agent},
        )

        self._parser = parser
        self.max_depth = max_depth
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}

        self.respect_robots = respect_robots
        self._user_agent = user_agent
        self._queue = None
        self._sem_manager = None
        self._rate_limiter = None
        self._robots_parser = None
        self._rps = requests_per_second
        self._min_delay = min_delay
        self._jitter = jitter
        self._per_domain_limit = per_domain_limit

        # FIX 3: task tracking — counts how many _crawl_one tasks are running
        self._active_tasks: int = 0
        # Event fires when active_tasks drops to zero AND queue is empty
        self._all_done_event: Optional[asyncio.Event] = None

        logger.info(
            "AsyncCrawler ready — concurrent=%d, depth=%d, rps=%.1f, robots=%s",
            max_concurrent, max_depth, requests_per_second, respect_robots,
        )

    # ================================================================== #
    #  Day 1 — HTTP fetch                                                 #
    #  PUBLIC INTERFACE: returns str (HTML) per course spec               #
    # ================================================================== #

    async def fetch_url(self, url: str) -> str:
        """
        Fetch ONE URL and return its HTML as a plain string.

        Returns "" (empty string) on any failure instead of raising,
        so callers can safely check: if not html: ...

        Per Day-1 spec: return type is str, not FetchResult.
        FetchResult is used internally by fetch_and_parse().
        """
        async with self._semaphore:
            result = await self._fetch_with_retry(url)
        return result.content or ""

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        """
        Fetch many URLs in parallel. Returns {url: html_string}.
        Per Day-1 spec: return type is dict[str, str].
        """
        if not urls:
            return {}
        t0 = time.perf_counter()
        html_list: list[str] = await asyncio.gather(
            *[self.fetch_url(u) for u in urls]
        )
        ok = sum(1 for h in html_list if h)
        logger.info("Batch done — %d OK / %d FAILED — %.2fs",
                    ok, len(html_list) - ok, time.perf_counter() - t0)
        return dict(zip(urls, html_list))

    # ── Internal fetch helpers (keep FetchResult for metadata) ──────── #

    async def _fetch_url_internal(self, url: str) -> FetchResult:
        """
        Internal version that returns a full FetchResult (with status code,
        content-type, elapsed). Used by fetch_and_parse() so it can attach
        status_code and content_type to the page dict.
        """
        async with self._semaphore:
            return await self._fetch_with_retry(url)

    async def _fetch_with_retry(self, url: str) -> FetchResult:
        """Retry wrapper: retries network errors, not HTTP 4xx/5xx."""
        last: Optional[FetchResult] = None
        for attempt in range(1, self.max_retries + 2):
            result = await self._do_fetch(url, attempt)
            if result.success:
                return result
            last = result
            # HTTP error (status set) = deterministic, no retry
            if result.status is not None:
                return result
            if attempt <= self.max_retries:
                logger.warning("Retrying %s (attempt %d)", url, attempt + 1)
                await asyncio.sleep(self.retry_delay)
        return last

    async def _do_fetch(self, url: str, attempt: int) -> FetchResult:
        """One raw HTTP GET → FetchResult with status, content-type, content."""
        logger.info("→ Fetching %s (attempt %d)", url, attempt)
        t0 = time.perf_counter()
        try:
            async with self._session.get(url, allow_redirects=True) as resp:
                resp.raise_for_status()
                content = await resp.text()
                elapsed = time.perf_counter() - t0
                # Extract base content-type without encoding suffix
                ct = resp.headers.get("Content-Type", "text/html")
                ct_base = ct.split(";")[0].strip()
                logger.info("✓ %s — HTTP %d | %.1f KB | %.2fs",
                            url, resp.status, len(content) / 1024, elapsed)
                return FetchResult(
                    url=url, status=resp.status,
                    content=content, content_type=ct_base, elapsed=elapsed,
                )
        except aiohttp.ClientResponseError as exc:
            elapsed = time.perf_counter() - t0
            logger.warning("✗ HTTP %d — %s (%.2fs)", exc.status, url, elapsed)
            return FetchResult(url=url, status=exc.status, error=str(exc), elapsed=elapsed)
        except asyncio.TimeoutError:
            elapsed = time.perf_counter() - t0
            logger.warning("✗ Timeout — %s (%.2fs)", url, elapsed)
            return FetchResult(url=url, error="TimeoutError", elapsed=elapsed)
        except aiohttp.ClientError as exc:
            elapsed = time.perf_counter() - t0
            logger.warning("✗ ClientError — %s: %s (%.2fs)", url, exc, elapsed)
            return FetchResult(url=url, error=f"ClientError: {exc}", elapsed=elapsed)

    # ================================================================== #
    #  Day 2 — Fetch + parse                                              #
    # ================================================================== #

    def _get_parser(self):
        if self._parser is None:
            from parser import HTMLParser
            self._parser = HTMLParser()
        return self._parser

    async def fetch_and_parse(self, url: str) -> dict:
        """
        Fetch a URL and parse its HTML. Returns a structured dict.

        FIX: now stamps status_code, content_type, crawled_at so that
        DataStorage._prepare_record() gets real values instead of defaults,
        and CrawlerStats.record_page() can build a real status-code distribution.
        """
        # Use internal version to get the full FetchResult (with metadata)
        result = await self._fetch_url_internal(url)

        if not result.success:
            from parser import _empty_page_data
            empty = _empty_page_data(url)
            empty["error"] = result.error or f"HTTP {result.status}"
            # Still stamp the real HTTP status even on failure
            empty["status_code"]  = result.status
            empty["content_type"] = result.content_type or "text/html"
            empty["crawled_at"]   = datetime.now(timezone.utc).isoformat()
            return empty

        page = await self._get_parser().parse_html(result.content, url)

        # FIX: attach HTTP metadata that storage and stats need
        page["fetch_elapsed"] = result.elapsed
        page["status_code"]   = result.status          # real HTTP code, e.g. 200
        page["content_type"]  = result.content_type or "text/html"
        page["crawled_at"]    = datetime.now(timezone.utc).isoformat()
        return page

    async def fetch_and_parse_urls(self, urls: list[str]) -> dict[str, dict]:
        """Parallel fetch+parse for a list of URLs."""
        if not urls:
            return {}
        results: list[dict] = await asyncio.gather(
            *[self.fetch_and_parse(u) for u in urls]
        )
        return {r["url"]: r for r in results}

    # ================================================================== #
    #  Day 3 — Full site crawl                                           #
    # ================================================================== #

    def _init_crawl_components(self) -> None:
        from queue_manager import CrawlerQueue, SemaphoreManager
        from rate_limiter import RateLimiter, RobotsParser

        self._queue = CrawlerQueue()
        self._sem_manager = SemaphoreManager(
            global_limit=self.max_concurrent,
            per_domain_limit=self._per_domain_limit,
        )
        self._rate_limiter = RateLimiter(
            requests_per_second=self._rps,
            per_domain=True,
            min_delay=self._min_delay,
            jitter=self._jitter,
        )
        if self.respect_robots:
            self._robots_parser = RobotsParser(user_agent=self._user_agent)

        # FIX 3: create the Event inside the running event loop
        self._all_done_event = asyncio.Event()

    def _should_crawl(self, url, seed_domain, same_domain_only,
                      exclude_patterns, include_patterns) -> bool:
        if self._queue.is_known(url):
            return False
        parsed = urlparse(url)
        if same_domain_only and parsed.netloc != seed_domain:
            return False
        for pat in exclude_patterns:
            if re.search(pat, url):
                return False
        if include_patterns:
            if not any(re.search(pat, url) for pat in include_patterns):
                return False
        return True

    async def _crawl_one(self, url: str, depth: int, seed_domain: str,
                         same_domain_only: bool, exclude_patterns: list[str],
                         include_patterns: list[str]) -> None:
        """
        Worker coroutine for one URL.
        FIX 3: increments/decrements _active_tasks and signals _all_done_event.
        """
        # Signal: this task is now running
        self._active_tasks += 1
        try:
            # ── robots.txt ────────────────────────────────────────────
            if self._robots_parser:
                rfp = await self._robots_parser.fetch_robots(self._session, url)
                if not self._robots_parser.can_fetch(rfp, url):
                    logger.info("robots.txt SKIP: %s", url)
                    self._queue.mark_failed(url, "blocked by robots.txt")
                    return
                crawl_delay = self._robots_parser.get_crawl_delay(rfp)
                if crawl_delay:
                    await asyncio.sleep(crawl_delay)

            # ── rate limiter ──────────────────────────────────────────
            await self._rate_limiter.acquire(url)

            # ── domain semaphore ──────────────────────────────────────
            ctx = await self._sem_manager.domain_context(url)
            async with ctx:
                page = await self.fetch_and_parse(url)

            # ── handle result ─────────────────────────────────────────
            if page.get("error"):
                self._queue.mark_failed(url, page["error"])
                self.failed_urls[url] = page["error"]
                return

            self._queue.mark_visited(url)
            self.visited_urls.add(url)
            self.processed_urls[url] = page

            # ── enqueue discovered links ──────────────────────────────
            if depth < self.max_depth:
                for link in page.get("links", []):
                    if self._should_crawl(link, seed_domain, same_domain_only,
                                          exclude_patterns, include_patterns):
                        await self._queue.add_url(link, depth=depth + 1)

        finally:
            # FIX 3: always decrement, even on exception
            self._active_tasks -= 1
            # If the queue is empty AND no tasks are running, signal done
            if self._active_tasks == 0 and self._queue.pending_count() == 0:
                self._all_done_event.set()

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = True,
        exclude_patterns: Optional[list[str]] = None,
        include_patterns: Optional[list[str]] = None,
    ) -> dict[str, dict]:
        """
        Main crawl loop.

        FIX 3: proper termination — instead of a fixed sleep(1.5) we wait on
        _all_done_event which fires only when BOTH conditions are true:
          a) the queue is empty (no more URLs to start)
          b) _active_tasks == 0 (all running workers have finished)
        This prevents exiting while workers are still running and may add URLs.
        """
        exclude_patterns = exclude_patterns or []
        include_patterns = include_patterns or []
        seed_domain = urlparse(start_urls[0]).netloc if start_urls else ""

        self._init_crawl_components()

        for url in start_urls:
            await self._queue.add_url(url, depth=0)

        t0 = time.perf_counter()
        last_log = t0
        logger.info("Crawl started — seed=%s, max_pages=%d, max_depth=%d",
                    seed_domain, max_pages, self.max_depth)

        while True:
            if len(self.visited_urls) >= max_pages:
                logger.info("Reached max_pages=%d, stopping.", max_pages)
                break

            item = await self._queue.get_next()

            if item is None:
                # Queue is empty right now.
                # If no workers are running either → truly finished.
                if self._active_tasks == 0:
                    break
                # Workers are still running and may add new URLs.
                # Wait until they either finish or add something.
                try:
                    # Wait for done-signal with a short timeout so we can
                    # also re-check max_pages periodically.
                    await asyncio.wait_for(
                        self._all_done_event.wait(), timeout=0.2
                    )
                    # Event fired → all workers done AND queue empty → stop
                    break
                except asyncio.TimeoutError:
                    # Timeout just means "check again" — not an error
                    self._all_done_event.clear()
                    continue

            # Reset the event before launching a new task
            self._all_done_event.clear()

            url = item   # get_next() returns str|None per Day-3 spec
            depth = self._queue.get_depth(url)  # depth stored in URLRecord
            asyncio.create_task(
                self._crawl_one(url, depth, seed_domain,
                                same_domain_only, exclude_patterns, include_patterns)
            )

            now = time.perf_counter()
            if now - last_log >= 5:
                stats = self._queue.get_stats()
                elapsed = now - t0
                speed = len(self.visited_urls) / elapsed if elapsed else 0
                logger.info(
                    "Progress — visited=%d | queued=%d | active=%d | failed=%d | %.1f p/s",
                    stats["visited"], stats["pending"],
                    self._active_tasks, stats["failed"], speed,
                )
                last_log = now

        elapsed = time.perf_counter() - t0
        logger.info(
            "Crawl done — %d pages in %.1fs (%.1f p/s) | %d failed",
            len(self.visited_urls), elapsed,
            len(self.visited_urls) / elapsed if elapsed else 0,
            len(self.failed_urls),
        )
        return self.processed_urls

    def get_crawl_stats(self) -> dict:
        stats = {}
        if self._queue:
            stats["queue"] = self._queue.get_stats()
        if self._rate_limiter:
            stats["rate_limiter"] = self._rate_limiter.get_stats()
        if self._robots_parser:
            stats["robots"] = self._robots_parser.get_stats()
        return stats

    async def close(self) -> None:
        await self._session.close()
        await asyncio.sleep(0)
        logger.info("AsyncCrawler session closed.")

    async def __aenter__(self) -> "AsyncCrawler":
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()
