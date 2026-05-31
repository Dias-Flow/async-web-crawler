"""
async_crawler/crawler.py  (Days 1-4 + Day 6 storage, final corrected version)

PUBLIC API per course spec:
  Day 1: fetch_url(url)  -> str            (HTML text, "" on failure)
         fetch_urls(urls) -> dict[str, str]
  Day 2: fetch_and_parse(url) -> dict
         fetch_and_parse_urls(urls) -> dict[str, dict]
  Day 3: crawl(...) -> dict[str, dict]
  Day 6: __init__ accepts  storage=  parameter; auto-saves after every page

INTERNAL helpers (not part of public API):
  _fetch_url_internal(url)   -> FetchResult  (used by fetch_and_parse)
  _do_fetch(url, attempt)    -> FetchResult  (catches all errors, never raises)
  _do_fetch_raising(url)     -> FetchResult  (raises on error, for RetryStrategy)
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
# RetryStrategy imported lazily inside __init__ to avoid circular imports

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("AsyncCrawler")


@dataclass
class FetchResult:
    """
    Internal container for one HTTP response.
    NOT part of public API — fetch_url() returns str.
    Used internally so fetch_and_parse() can access status_code and content_type.
    """
    url: str
    status: Optional[int] = None
    content: Optional[str] = None
    content_type: Optional[str] = None
    error: Optional[str] = None
    elapsed: float = 0.0

    @property
    def success(self) -> bool:
        return self.error is None and self.content is not None


class AsyncCrawler:

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
        storage=None,   # Day 6: DataStorage instance, auto-saves after each page
        retry_strategy=None,  # Day 5: RetryStrategy; created with defaults if None
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

        # Day 6: storage lives in AsyncCrawler per spec
        self.storage = storage

        # Day 5: RetryStrategy is part of AsyncCrawler per spec.
        # If none provided, create a default one with the given max_retries.
        # This handles transient errors (503, 429, timeout) with exponential
        # backoff so the crawler is polite under load.
        if retry_strategy is None:
            from retry_strategy import RetryStrategy
            self.retry_strategy = RetryStrategy(
                max_retries=max_retries,
                backoff_base=1.0,
                backoff_factor=2.0,
                max_backoff=60.0,
            )
        else:
            self.retry_strategy = retry_strategy

        self._queue = None
        self._sem_manager = None
        self._rps = requests_per_second
        self._min_delay = min_delay
        self._jitter = jitter
        self._per_domain_limit = per_domain_limit

        # Day 4: create eagerly so fetch_url/fetch_urls also respect
        # rate limits and robots.txt, not just crawl().
        from rate_limiter import RateLimiter, RobotsParser
        self._rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            per_domain=True,
            min_delay=min_delay,
            jitter=jitter,
        )
        self._robots_parser = (
            RobotsParser(user_agent=user_agent) if respect_robots else None
        )

        self._active_tasks: int = 0
        self._all_done_event: Optional[asyncio.Event] = None

        logger.info(
            "AsyncCrawler ready - concurrent=%d, depth=%d, rps=%.1f, robots=%s",
            max_concurrent, max_depth, requests_per_second, respect_robots,
        )

    # ================================================================== #
    #  Day 1 - public API returns str per spec                            #
    # ================================================================== #

    async def fetch_url(self, url: str) -> str:
        """
        Fetch one URL. Returns HTML as str, or "" on failure.
        Per Day-1 spec: return type is str.
        """
        async with self._semaphore:
            result = await self._fetch_with_retry(url)
        return result.content or ""

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        """
        Fetch many URLs in parallel. Returns {url: html_str}.
        Per Day-1 spec: return type is dict[str, str].
        """
        if not urls:
            return {}
        t0 = time.perf_counter()
        html_list: list[str] = await asyncio.gather(
            *[self.fetch_url(u) for u in urls]
        )
        ok = sum(1 for h in html_list if h)
        logger.info("Batch done - %d OK / %d FAILED - %.2fs",
                    ok, len(html_list) - ok, time.perf_counter() - t0)
        return dict(zip(urls, html_list))

    # ================================================================== #
    #  Internal helpers                                                    #
    # ================================================================== #

    async def _fetch_url_internal(self, url: str) -> FetchResult:
        """
        Internal fetch that returns a full FetchResult (includes status_code,
        content_type, elapsed). Used by fetch_and_parse() and AdvancedCrawler.
        """
        async with self._semaphore:
            return await self._fetch_with_retry(url)

    async def _fetch_with_retry(self, url: str) -> FetchResult:
        """
        Retry loop using self.retry_strategy (Day 5 integration).

        Uses _do_fetch_raising so RetryStrategy can see real exceptions and:
          - retry 503 / 429 / timeout with exponential backoff  (TransientError)
          - retry DNS / connection errors                        (NetworkError)
          - NOT retry 404 / 403                                  (PermanentError)

        On permanent failure or exhausted retries, catches the exception and
        returns a FetchResult(error=...) so callers never have to handle exceptions.
        """
        from retry_strategy import CrawlerError
        try:
            return await self.retry_strategy.execute_with_retry(
                self._do_fetch_raising, url, url=url
            )
        except CrawlerError as exc:
            # Permanent error or all retries exhausted — map back to FetchResult
            status = getattr(exc.__cause__, "status", None) if exc.__cause__ else None
            # Try to extract HTTP status from the message string as fallback
            if status is None:
                import re as _re
                m = _re.search(r"HTTP (\d+)", str(exc))
                status = int(m.group(1)) if m else None
            return FetchResult(url=url, status=status, error=str(exc))
        except Exception as exc:
            return FetchResult(url=url, error=f"Unexpected: {exc}")

    async def _do_fetch(self, url: str, attempt: int) -> FetchResult:
        """
        One HTTP GET. NEVER raises - all exceptions become FetchResult.error.
        Used by the normal retry loop inside fetch_url / fetch_urls.
        """
        logger.info("-> Fetching %s (attempt %d)", url, attempt)
        # Apply politeness before every request (rate limit + robots + Crawl-delay)
        await self._rate_limiter.acquire(url)
        if self._robots_parser:
            await self._robots_parser.fetch_robots(url)
            if not self._robots_parser.can_fetch(url):
                from retry_strategy import PermanentError
                raise PermanentError(f"robots.txt disallows: {url}")
            crawl_delay = self._robots_parser.get_crawl_delay()
            if crawl_delay:
                await asyncio.sleep(crawl_delay)
        t0 = time.perf_counter()
        try:
            async with self._session.get(url, allow_redirects=True) as resp:
                resp.raise_for_status()
                content = await resp.text()
                elapsed = time.perf_counter() - t0
                ct_header = resp.headers.get("Content-Type", "text/html")
                # resp.headers.get can return a coroutine in tests if headers is AsyncMock
                if not isinstance(ct_header, str):
                    ct_header = "text/html"
                ct = ct_header.split(";")[0].strip()
                logger.info("OK %s - HTTP %d | %.1f KB | %.2fs",
                            url, resp.status, len(content) / 1024, elapsed)
                return FetchResult(url=url, status=resp.status, content=content,
                                   content_type=ct, elapsed=elapsed)
        except aiohttp.ClientResponseError as exc:
            elapsed = time.perf_counter() - t0
            logger.warning("FAIL HTTP %d - %s (%.2fs)", exc.status, url, elapsed)
            return FetchResult(url=url, status=exc.status, error=str(exc), elapsed=elapsed)
        except asyncio.TimeoutError:
            elapsed = time.perf_counter() - t0
            logger.warning("FAIL Timeout - %s (%.2fs)", url, elapsed)
            return FetchResult(url=url, error="TimeoutError", elapsed=elapsed)
        except aiohttp.ClientError as exc:
            elapsed = time.perf_counter() - t0
            logger.warning("FAIL ClientError - %s: %s (%.2fs)", url, exc, elapsed)
            return FetchResult(url=url, error=f"ClientError: {exc}", elapsed=elapsed)

    async def _do_fetch_raising(self, url: str) -> FetchResult:
        """
        One HTTP GET. RAISES the original exception on failure.

        WHY this version exists:
          RetryStrategy.execute_with_retry() only retries when the wrapped
          function raises an exception. _do_fetch() swallows all errors into
          FetchResult(error=...) so execute_with_retry() never triggers.

          _do_fetch_raising() re-raises so RetryStrategy can:
            - catch aiohttp.ClientResponseError and classify it
              (429/503 -> TransientError -> retry with backoff)
            - catch asyncio.TimeoutError -> TransientError -> retry
            - catch aiohttp.ClientConnectorError -> NetworkError -> retry
            - catch 404 -> PermanentError -> do NOT retry

          Used exclusively by AdvancedCrawler._crawl_one_with_retry().
        """
        logger.info("-> Fetching (raising) %s", url)
        # Apply rate limiting, robots check, and Crawl-delay
        await self._rate_limiter.acquire(url)
        if self._robots_parser:
            await self._robots_parser.fetch_robots(url)
            if not self._robots_parser.can_fetch(url):
                from retry_strategy import PermanentError
                raise PermanentError(f"robots.txt disallows: {url}")
            # Honour Crawl-delay from robots.txt (Day-4 requirement)
            crawl_delay = self._robots_parser.get_crawl_delay()
            if crawl_delay:
                await asyncio.sleep(crawl_delay)
        t0 = time.perf_counter()  # start timing AFTER politeness checks
        async with self._session.get(url, allow_redirects=True) as resp:
            resp.raise_for_status()
            content = await resp.text()
            elapsed = time.perf_counter() - t0
            ct_header = resp.headers.get("Content-Type", "text/html")
            if not isinstance(ct_header, str):
                ct_header = "text/html"
            ct = ct_header.split(";")[0].strip()
            logger.info("OK %s - HTTP %d | %.1f KB | %.2fs",
                        url, resp.status, len(content) / 1024, elapsed)
            return FetchResult(url=url, status=resp.status, content=content,
                               content_type=ct, elapsed=elapsed)

    # ================================================================== #
    #  Day 2 - fetch + parse                                              #
    # ================================================================== #

    def _get_parser(self):
        if self._parser is None:
            from parser import HTMLParser
            self._parser = HTMLParser()
        return self._parser

    async def fetch_and_parse(self, url: str) -> dict:
        """
        Fetch a URL and return a structured page dict.
        Always sets status_code, content_type, crawled_at.
        """
        result = await self._fetch_url_internal(url)

        if not result.success:
            from parser import _empty_page_data
            empty = _empty_page_data(url)
            empty["error"]        = result.error or f"HTTP {result.status}"
            empty["status_code"]  = result.status
            empty["content_type"] = result.content_type or "text/html"
            empty["crawled_at"]   = datetime.now(timezone.utc).isoformat()
            return empty

        page = await self._get_parser().parse_html(result.content, url)
        page["fetch_elapsed"] = result.elapsed
        page["status_code"]   = result.status
        page["content_type"]  = result.content_type or "text/html"
        page["crawled_at"]    = datetime.now(timezone.utc).isoformat()
        return page

    async def fetch_and_parse_urls(self, urls: list[str]) -> dict[str, dict]:
        if not urls:
            return {}
        results: list[dict] = await asyncio.gather(
            *[self.fetch_and_parse(u) for u in urls]
        )
        return {r["url"]: r for r in results}

    # ================================================================== #
    #  Day 3 - queue-driven crawl                                         #
    # ================================================================== #

    def _init_crawl_components(self) -> None:
        from queue_manager import CrawlerQueue, SemaphoreManager

        self._queue = CrawlerQueue()
        self._sem_manager = SemaphoreManager(
            global_limit=self.max_concurrent,
            per_domain_limit=self._per_domain_limit,
        )
        # _rate_limiter and _robots_parser already created in __init__
        self._all_done_event = asyncio.Event()
        self._crawl_tasks: set[asyncio.Task] = set()  # track every spawned task

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
                         same_domain_only: bool, exclude_patterns: list,
                         include_patterns: list) -> None:
        """
        Worker coroutine for one URL.

        Important: crawl() increments self._active_tasks immediately before
        scheduling this coroutine. This avoids a race where the main crawl loop
        could see zero active tasks before a newly created task has actually
        started. This method only decrements the counter in finally.
        """
        try:
            # Rate limiting and robots.txt are applied inside _do_fetch_raising()
            # via fetch_and_parse(). We only do a cached pre-check here so a
            # disallowed URL is recorded without spending a retry slot.
            if self._robots_parser:
                await self._robots_parser.fetch_robots(url)
                if not self._robots_parser.can_fetch(url):
                    logger.info("robots.txt SKIP: %s", url)
                    self._queue.mark_processed(url, error="blocked by robots.txt")
                    self.failed_urls[url] = "blocked by robots.txt"
                    return

            ctx = await self._sem_manager.domain_context(url)
            async with ctx:
                page = await self.fetch_and_parse(url)

            if page.get("error"):
                self._queue.mark_processed(url, error=page["error"])
                self.failed_urls[url] = page["error"]
                return

            self._queue.mark_processed(url)
            self.visited_urls.add(url)
            self.processed_urls[url] = page

            # Day 6: auto-save after every page
            if self.storage:
                try:
                    await self.storage.save(page)
                except Exception as exc:
                    logger.error("Storage save failed for %s: %s", url, exc)

            if depth < self.max_depth:
                for link in page.get("links", []):
                    if self._should_crawl(link, seed_domain, same_domain_only,
                                          exclude_patterns, include_patterns):
                        self._queue.add_url(link, depth=depth + 1)

        except asyncio.CancelledError:
            # If the crawl is cancelled from outside, keep queue bookkeeping sane.
            if self._queue:
                self._queue.mark_processed(url, error="cancelled")
                self.failed_urls[url] = "cancelled"
            raise
        except Exception as exc:
            logger.exception("Unexpected crawl worker error for %s: %s", url, exc)
            if self._queue:
                self._queue.mark_processed(url, error=str(exc))
            self.failed_urls[url] = str(exc)
        finally:
            self._active_tasks -= 1
            if self._all_done_event and self._active_tasks == 0 and self._queue.pending_count() == 0:
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
        Crawl pages from start_urls with a strict max_pages scheduling limit.

        The previous implementation counted only visited + active tasks. Because
        active_tasks was incremented inside worker coroutines, the controller
        could exit too early or overschedule pages. This version increments
        active_tasks before create_task() and uses scheduled_count as the hard
        limit: no more than max_pages URLs are ever scheduled.
        """
        exclude_patterns = exclude_patterns or []
        include_patterns = include_patterns or []
        seed_domain = urlparse(start_urls[0]).netloc if start_urls else ""

        self._init_crawl_components()

        for url in start_urls:
            self._queue.add_url(url, depth=0)

        t0 = time.perf_counter()
        last_log = t0
        scheduled_count = 0
        logger.info("Crawl started - seed=%s, max_pages=%d, max_depth=%d",
                    seed_domain, max_pages, self.max_depth)

        while True:
            # Stop taking new URLs once the strict scheduling budget is used.
            if scheduled_count >= max_pages:
                logger.info("Reached max_pages=%d; waiting for in-flight tasks.", max_pages)
                break

            url = await self._queue.get_next()  # str|None per Day-3 spec

            if url is None:
                if self._active_tasks == 0:
                    break
                try:
                    await asyncio.wait_for(self._all_done_event.wait(), timeout=0.2)
                except asyncio.TimeoutError:
                    pass
                finally:
                    self._all_done_event.clear()
                continue

            depth = self._queue.get_depth(url)
            self._all_done_event.clear()

            # Count the task BEFORE scheduling it. This removes the race where
            # create_task() has returned but the coroutine has not yet incremented
            # _active_tasks.
            self._active_tasks += 1
            scheduled_count += 1
            task = asyncio.create_task(
                self._crawl_one(url, depth, seed_domain,
                                same_domain_only, exclude_patterns, include_patterns)
            )
            self._crawl_tasks.add(task)
            task.add_done_callback(self._crawl_tasks.discard)

            now = time.perf_counter()
            if now - last_log >= 5:
                stats = self._queue.get_stats()
                elapsed = now - t0
                speed = len(self.visited_urls) / elapsed if elapsed else 0
                logger.info(
                    "Progress - visited=%d queued=%d active=%d failed=%d scheduled=%d %.1f p/s",
                    stats["visited"], stats["pending"],
                    self._active_tasks, stats["failed"], scheduled_count, speed,
                )
                last_log = now

        # Wait for every in-flight worker to finish before returning.
        if self._crawl_tasks:
            logger.info("Waiting for %d in-flight tasks...", len(self._crawl_tasks))
            await asyncio.gather(*list(self._crawl_tasks), return_exceptions=True)

        elapsed = time.perf_counter() - t0
        logger.info(
            "Crawl done - %d pages in %.1fs (%.1f p/s) | %d failed | %d scheduled",
            len(self.visited_urls), elapsed,
            len(self.visited_urls) / elapsed if elapsed else 0,
            len(self.failed_urls), scheduled_count,
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
        # Fix: flush and close storage BEFORE closing the session,
        # so any final SQLite batch flush can still log via the session if needed.
        # Without this, SQLiteStorage buffer is lost on process exit.
        if self.storage:
            try:
                await self.storage.close()
            except Exception as exc:
                logger.error("Storage close error: %s", exc)
        if self._robots_parser:
            await self._robots_parser.close()
        await self._session.close()
        await asyncio.sleep(0)
        logger.info("AsyncCrawler session closed.")

    async def __aenter__(self) -> "AsyncCrawler":
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()


# ---------------------------------------------------------------------------
# Day-7 re-export: per spec example 'from crawler import AdvancedCrawler'
# ---------------------------------------------------------------------------
from advanced_crawler import AdvancedCrawler  # noqa: F401
