"""
async_crawler/crawler.py  (Days 1 + 2 + 3 + 4)

This is the CENTRAL file of the project.
Every other module plugs into this class.

Reading order for a beginner:
  1. FetchResult dataclass  — understand what one HTTP request returns
  2. __init__               — understand how the crawler is configured
  3. fetch_url / _do_fetch  — understand one raw HTTP request
  4. fetch_urls             — understand how parallel requests work
  5. fetch_and_parse        — Day 2: HTTP + HTML parsing in one call
  6. crawl()                — Day 3: full site traversal
"""

import asyncio
import logging
import re
import time
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import aiohttp

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
# logging.basicConfig sets the FORMAT of every log line across the whole app.
# %(asctime)s  = timestamp like "14:23:01"
# %(levelname)s = INFO / WARNING / ERROR
# %(name)s     = which class logged it, e.g. "AsyncCrawler" or "HTMLParser"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("AsyncCrawler")


# ===========================================================================
# FetchResult — the return type of every HTTP request
# ===========================================================================
@dataclass
class FetchResult:
    """
    A simple container that holds the outcome of one HTTP GET request.

    WHY a dataclass and not a plain dict?
      - Autocomplete in PyCharm (you can type result. and see all fields)
      - Type safety: result.status is always int|None, never accidentally a string
      - The .success property gives a clean True/False without if-chains everywhere

    FIELDS:
      url     — the URL that was requested
      status  — HTTP status code (200, 404, 500…); None if we never got a response
      content — the raw HTML text; None if the request failed
      error   — human-readable error description; None if request succeeded
      elapsed — how many seconds the request took
    """
    url: str
    status: Optional[int] = None
    content: Optional[str] = None
    error: Optional[str] = None
    elapsed: float = 0.0

    @property
    def success(self) -> bool:
        # A request is considered successful ONLY if:
        #   - we have actual content (HTML was returned)
        #   - AND there is no error message
        # A 200 response with empty content still counts as failed.
        return self.error is None and self.content is not None


# ===========================================================================
# AsyncCrawler — the main class
# ===========================================================================
class AsyncCrawler:
    """
    The core crawler class. Grows across 4 days:

    Day 1: Can fetch URLs in parallel with error handling
    Day 2: Can also parse the HTML it fetches
    Day 3: Can crawl a whole site by following links
    Day 4: Respects robots.txt and rate limits
    """

    def __init__(
        self,
        # ── Day 1 params ──────────────────────────────────────────────
        max_concurrent: int = 10,       # max requests running at the same time
        connect_timeout: float = 10.0,  # seconds to wait for TCP connection
        read_timeout: float = 30.0,     # seconds to wait for the full page to download
        max_retries: int = 2,           # how many times to retry a failed request
        retry_delay: float = 1.0,       # seconds to wait between retries
        # ── Day 2 params ──────────────────────────────────────────────
        parser=None,                    # inject an HTMLParser, or let it be created lazily
        # ── Day 3 params ──────────────────────────────────────────────
        max_depth: int = 2,             # 0 = seed only, 1 = seed + its links, 2 = two hops
        per_domain_limit: int = 3,      # max simultaneous requests to ONE domain
        # ── Day 4 params ──────────────────────────────────────────────
        requests_per_second: float = 1.0,   # politeness: 1 request/sec per domain
        min_delay: float = 0.5,             # absolute minimum gap between requests
        jitter: float = 0.3,                # random extra delay (looks more human)
        respect_robots: bool = True,        # read and obey robots.txt
        user_agent: str = "AsyncCrawler/1.0 (educational project; aiohttp)",
    ) -> None:

        # ── Day 1 setup ───────────────────────────────────────────────
        self.max_concurrent = max_concurrent
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # asyncio.Semaphore is like a "ticket booth".
        # max_concurrent=10 means 10 tickets exist.
        # Each coroutine must grab a ticket before making a request.
        # If all 10 tickets are taken, the next coroutine WAITS here
        # until someone returns a ticket (finishes their request).
        self._semaphore = asyncio.Semaphore(max_concurrent)

        # aiohttp.ClientTimeout bundles connect + read timeouts together.
        # Without timeouts, a hung server would freeze the crawler forever.
        self._timeout = aiohttp.ClientTimeout(
            connect=connect_timeout,
            total=read_timeout,
        )

        # TCPConnector manages the pool of open TCP connections.
        # WHY a connection pool?
        #   Opening a new TCP connection (SYN → SYN-ACK → ACK + TLS handshake)
        #   takes ~100-500ms. Reusing an existing connection costs ~0ms.
        #   Without pooling, 100 requests = 100 connection setups = slow.
        connector = aiohttp.TCPConnector(
            limit=max_concurrent * 2,       # total open sockets across all hosts
            limit_per_host=per_domain_limit, # max sockets to one specific host
            ttl_dns_cache=300,              # cache DNS lookups for 5 minutes
            enable_cleanup_closed=True,     # free memory from closed connections
        )

        # ClientSession is the actual HTTP client.
        # ONE session for the whole crawler lifetime = connection reuse.
        # Creating a new session per request would be very slow and wasteful.
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=self._timeout,
            headers={"User-Agent": user_agent},
        )

        # ── Day 2 setup ───────────────────────────────────────────────
        # We store the parser but don't import HTMLParser here.
        # WHY? Because not every user needs parsing.
        # The import only happens on first call to _get_parser().
        self._parser = parser

        # ── Day 3 setup ───────────────────────────────────────────────
        self.max_depth = max_depth

        # These three dicts/sets are the "memory" of the crawl.
        # visited_urls: every URL we successfully processed (set = no duplicates)
        # failed_urls:  every URL that ultimately failed (url → error message)
        # processed_urls: full parsed data for each success (url → page dict)
        self.visited_urls: set[str] = set()
        self.failed_urls: dict[str, str] = {}
        self.processed_urls: dict[str, dict] = {}

        # ── Day 4 setup ───────────────────────────────────────────────
        self.respect_robots = respect_robots
        self._user_agent = user_agent

        # Day 3 and Day 4 objects are created lazily inside crawl().
        # WHY lazy? asyncio.Semaphore and asyncio.PriorityQueue must be created
        # AFTER asyncio.run() starts the event loop. If created in __init__
        # before asyncio.run() they can crash or attach to the wrong loop.
        self._queue = None
        self._sem_manager = None
        self._rate_limiter = None
        self._robots_parser = None

        # Store these for later when _init_crawl_components() is called
        self._rps = requests_per_second
        self._min_delay = min_delay
        self._jitter = jitter
        self._per_domain_limit = per_domain_limit

        logger.info(
            "AsyncCrawler ready — concurrent=%d, depth=%d, rps=%.1f, robots=%s",
            max_concurrent, max_depth, requests_per_second, respect_robots,
        )

    # ================================================================== #
    #  Day 1 — Low-level HTTP fetching                                    #
    # ================================================================== #

    async def fetch_url(self, url: str) -> FetchResult:
        """
        Fetch ONE URL. This is the foundation everything else builds on.

        The 'async with self._semaphore' line is the concurrency gate:
          - If fewer than max_concurrent requests are running → proceed immediately
          - If max_concurrent requests are already running → WAIT here until one finishes

        Think of it like a nightclub with max_concurrent=10:
          10 people inside? You wait at the door.
          Someone leaves? You get in.
        """
        async with self._semaphore:
            # We're inside the semaphore (have our "ticket")
            # Now call the retry wrapper
            return await self._fetch_with_retry(url)

    async def fetch_urls(self, urls: list[str]) -> dict[str, FetchResult]:
        """
        Fetch MANY URLs in parallel.

        asyncio.gather() is the key here.
        It takes a list of coroutines and runs them ALL concurrently.
        Each one internally calls fetch_url() which uses the semaphore
        to limit how many actually run at the same time.

        Without gather:
            for url in urls:
                await fetch_url(url)   # sequential: 10 urls × 1s = 10s

        With gather:
            await asyncio.gather(...)  # parallel: 10 urls × 1s = ~1s
        """
        if not urls:
            return {}
        t0 = time.perf_counter()

        # *[...] unpacks the list into separate arguments for gather()
        # gather() returns a list of results in the same order as inputs
        results: list[FetchResult] = await asyncio.gather(
            *[self.fetch_url(u) for u in urls]
        )

        ok = sum(1 for r in results if r.success)
        logger.info("Batch done — %d OK / %d FAILED — %.2fs",
                    ok, len(results) - ok, time.perf_counter() - t0)

        # Convert list → dict so callers can do results["https://example.com"]
        return {r.url: r for r in results}

    async def _fetch_with_retry(self, url: str) -> FetchResult:
        """
        Retry loop wrapper around _do_fetch().

        RETRY LOGIC:
          - Network errors (DNS fail, connection reset, timeout): YES retry
            These are temporary infrastructure problems that may fix themselves.
          - HTTP errors (404, 403, 500): NO retry (if status code is set)
            A 404 means "this page doesn't exist". Retrying won't help.
            A 403 means "you're not allowed". Retrying won't help.
            Exception: 503 is handled in Day 5's RetryStrategy.

        range(1, max_retries + 2) gives us: [1, 2, 3] for max_retries=2
        That's: attempt 1 (first try) + attempt 2 (retry 1) + attempt 3 (retry 2)
        """
        last: Optional[FetchResult] = None
        for attempt in range(1, self.max_retries + 2):
            result = await self._do_fetch(url, attempt)

            if result.success:
                return result  # 🎉 success, stop immediately

            last = result

            # If we got a status code, the server RESPONDED (just with an error).
            # That means the URL is reachable — retrying won't change the answer.
            if result.status is not None:
                return result

            # Network error (no status code) — might be temporary, retry
            if attempt <= self.max_retries:
                logger.warning("Retrying %s (attempt %d)", url, attempt + 1)
                await asyncio.sleep(self.retry_delay)

        return last  # return the last failure after all retries are exhausted

    async def _do_fetch(self, url: str, attempt: int) -> FetchResult:
        """
        Perform ONE raw HTTP GET request and wrap the result in FetchResult.

        'async with self._session.get(url) as resp:'
          This opens a connection, sends GET, waits for response headers.
          The 'async with' automatically closes/releases the connection after.

        'resp.raise_for_status()'
          Converts 4xx/5xx status codes into Python exceptions.
          Without this, aiohttp would silently give you a 404 page as "success".
          After this call, only 2xx responses continue past the line.

        'await resp.text()'
          Downloads the response BODY (the actual HTML).
          This is separate from headers — headers arrive first, body after.
          'await' means: don't block; let other coroutines run while we wait.

        The three except blocks handle different failure categories:
          ClientResponseError  → server replied with 4xx/5xx (raise_for_status triggered)
          TimeoutError         → server didn't respond in time
          ClientError          → couldn't even connect (DNS, TCP refused, etc.)
        """
        logger.info("→ Fetching %s (attempt %d)", url, attempt)
        t0 = time.perf_counter()

        try:
            async with self._session.get(url, allow_redirects=True) as resp:
                # allow_redirects=True follows 301/302 redirects automatically
                resp.raise_for_status()
                content = await resp.text()
                elapsed = time.perf_counter() - t0

                logger.info("✓ %s — HTTP %d | %.1f KB | %.2fs",
                            url, resp.status, len(content) / 1024, elapsed)

                return FetchResult(
                    url=url,
                    status=resp.status,
                    content=content,
                    elapsed=elapsed,
                )

        except aiohttp.ClientResponseError as exc:
            # Server responded, but with an error code (404, 500, etc.)
            elapsed = time.perf_counter() - t0
            logger.warning("✗ HTTP %d — %s (%.2fs)", exc.status, url, elapsed)
            return FetchResult(url=url, status=exc.status, error=str(exc), elapsed=elapsed)

        except asyncio.TimeoutError:
            # Server did not respond within read_timeout seconds
            elapsed = time.perf_counter() - t0
            logger.warning("✗ Timeout — %s (%.2fs)", url, elapsed)
            return FetchResult(url=url, error="TimeoutError", elapsed=elapsed)

        except aiohttp.ClientError as exc:
            # Could not connect at all (bad DNS, port closed, SSL error, etc.)
            elapsed = time.perf_counter() - t0
            logger.warning("✗ ClientError — %s: %s (%.2fs)", url, exc, elapsed)
            return FetchResult(url=url, error=f"ClientError: {exc}", elapsed=elapsed)

    # ================================================================== #
    #  Day 2 — Fetch + HTML parsing                                       #
    # ================================================================== #

    def _get_parser(self):
        """
        Return the HTMLParser instance, creating it on first call.

        WHY lazy creation (not in __init__)?
          1. Not every user of AsyncCrawler needs HTML parsing.
          2. The import is inside the method to avoid a circular import:
             parser.py imports nothing from crawler.py, so this direction is safe.
             But if we imported at the top of the file, and parser.py ever needed
             crawler.py, we'd get a circular import crash.
        """
        if self._parser is None:
            from parser import HTMLParser
            self._parser = HTMLParser()
        return self._parser

    async def fetch_and_parse(self, url: str) -> dict:
        """
        Convenience method: fetch a URL AND parse the HTML in one call.

        Returns a dict (not a FetchResult) with structured data:
          url, title, text, text_length, links, links_count,
          images, images_count, headings, metadata, fetch_elapsed, error

        Even on failure it returns the SAME shape (with error set).
        This means callers never need to handle two different return types:
            page = await crawler.fetch_and_parse(url)
            if page["error"]:
                ...  # same dict shape either way
        """
        # Step 1: Download the page (Day 1 logic)
        result = await self.fetch_url(url)

        if not result.success:
            # Import the "empty result" factory and fill in the error
            from parser import _empty_page_data
            empty = _empty_page_data(url)
            empty["error"] = result.error or f"HTTP {result.status}"
            return empty

        # Step 2: Parse the HTML (Day 2 logic)
        page = await self._get_parser().parse_html(result.content, url)

        # Attach network timing so callers can see both parse and fetch speed
        page["fetch_elapsed"] = result.elapsed
        return page

    async def fetch_and_parse_urls(self, urls: list[str]) -> dict[str, dict]:
        """
        Parallel version of fetch_and_parse for a list of URLs.
        Same pattern as fetch_urls but returns parsed dicts instead of FetchResults.
        """
        if not urls:
            return {}
        results: list[dict] = await asyncio.gather(
            *[self.fetch_and_parse(u) for u in urls]
        )
        return {r["url"]: r for r in results}

    # ================================================================== #
    #  Day 3 — Full site crawl with queue, depth, and URL filters         #
    # ================================================================== #

    def _init_crawl_components(self) -> None:
        """
        Create the Day 3/4 helper objects right before crawl() starts.

        WHY not in __init__?
        asyncio.Semaphore and asyncio.PriorityQueue MUST be created after
        the event loop is running (i.e., after asyncio.run() is called).
        If you create them before, they get attached to no loop and crash.

        _init_crawl_components() is called at the start of crawl(),
        which is always inside asyncio.run(), so it's safe.
        """
        from queue_manager import CrawlerQueue, SemaphoreManager
        from rate_limiter import RateLimiter, RobotsParser

        # CrawlerQueue tracks which URLs we've seen, which are pending,
        # which succeeded, which failed — it's the crawler's "memory"
        self._queue = CrawlerQueue()

        # SemaphoreManager gives us two levels of concurrency control:
        # global (total requests) AND per-domain (requests to one host)
        self._sem_manager = SemaphoreManager(
            global_limit=self.max_concurrent,
            per_domain_limit=self._per_domain_limit,
        )

        # RateLimiter enforces a minimum wait between requests to each domain
        self._rate_limiter = RateLimiter(
            requests_per_second=self._rps,
            per_domain=True,
            min_delay=self._min_delay,
            jitter=self._jitter,
        )

        # RobotsParser downloads and caches robots.txt for each domain
        if self.respect_robots:
            self._robots_parser = RobotsParser(user_agent=self._user_agent)

    def _should_crawl(
        self,
        url: str,
        seed_domain: str,
        same_domain_only: bool,
        exclude_patterns: list[str],
        include_patterns: list[str],
    ) -> bool:
        """
        Decide whether to add a discovered URL to the crawl queue.

        Returns True (add it) only if ALL four conditions pass:
          1. We've never seen this URL before (no duplicate work)
          2. It's on the same domain (if same_domain_only=True)
          3. It doesn't match any exclude_patterns (skip images, PDFs, etc.)
          4. It matches at least one include_pattern (if any patterns are defined)

        Example:
          exclude_patterns=["\\.pdf$"]  → skip all .pdf links
          include_patterns=["/blog/"]   → only follow links containing /blog/
        """
        # Rule 1: skip if already in queue (any state: pending/visited/failed)
        if self._queue.is_known(url):
            return False

        # Rule 2: stay on the same domain
        parsed = urlparse(url)
        if same_domain_only and parsed.netloc != seed_domain:
            return False

        # Rule 3: skip excluded patterns (regex match on the full URL string)
        for pat in exclude_patterns:
            if re.search(pat, url):
                return False

        # Rule 4: if include_patterns defined, at least one must match
        if include_patterns:
            if not any(re.search(pat, url) for pat in include_patterns):
                return False

        return True

    async def _crawl_one(
        self,
        url: str,
        depth: int,
        seed_domain: str,
        same_domain_only: bool,
        exclude_patterns: list[str],
        include_patterns: list[str],
    ) -> None:
        """
        Process a single URL from the queue.

        This is the "worker" coroutine. The crawl() loop spawns many of
        these running concurrently via asyncio.create_task().

        Processing steps in order:
          1. Check robots.txt — if blocked, mark failed and return
          2. Wait for rate limiter — be polite to the server
          3. Acquire domain semaphore — don't overwhelm one host
          4. Fetch + parse the page
          5. Save result, mark URL as visited
          6. Add newly discovered links to the queue (if not too deep)
        """
        # ── Step 1: robots.txt check (Day 4) ──────────────────────────
        if self._robots_parser:
            rfp = await self._robots_parser.fetch_robots(self._session, url)
            if not self._robots_parser.can_fetch(rfp, url):
                # This URL is explicitly forbidden by the site's robots.txt.
                # We mark it as failed (not visited) and skip it.
                logger.info("robots.txt SKIP: %s", url)
                self._queue.mark_failed(url, "blocked by robots.txt")
                return

            # Some robots.txt files specify a Crawl-delay.
            # If the site asks for 5s delay, we must honour that.
            crawl_delay = self._robots_parser.get_crawl_delay(rfp)
            if crawl_delay:
                await asyncio.sleep(crawl_delay)

        # ── Step 2: rate limiter (Day 4) ──────────────────────────────
        # This may sleep for 0.5–1.5 seconds to respect requests_per_second.
        # The sleep is per-domain, so different domains don't block each other.
        await self._rate_limiter.acquire(url)

        # ── Step 3: domain semaphore (Day 3) ──────────────────────────
        # Limits simultaneous requests to one host, e.g. max 3 to example.com
        ctx = await self._sem_manager.domain_context(url)
        async with ctx:
            # ── Step 4: fetch + parse (Days 1 + 2) ────────────────────
            page = await self.fetch_and_parse(url)

        # ── Step 5: handle result ──────────────────────────────────────
        if page.get("error"):
            self._queue.mark_failed(url, page["error"])
            self.failed_urls[url] = page["error"]
            return

        self._queue.mark_visited(url)
        self.visited_urls.add(url)
        self.processed_urls[url] = page

        # ── Step 6: enqueue discovered links (Day 3) ───────────────────
        # Only if we haven't reached max_depth yet.
        # depth=0 is the seed, depth=1 is one hop away, etc.
        if depth < self.max_depth:
            for link in page.get("links", []):
                if self._should_crawl(link, seed_domain, same_domain_only,
                                      exclude_patterns, include_patterns):
                    await self._queue.add_url(link, depth=depth + 1)

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = True,
        exclude_patterns: Optional[list[str]] = None,
        include_patterns: Optional[list[str]] = None,
    ) -> dict[str, dict]:
        """
        Main crawl loop. Seeds the queue and processes URLs until done.

        HOW THE LOOP WORKS:
          1. Seed URLs go into the queue at depth=0
          2. Pop the next URL from the queue
          3. Launch _crawl_one() as a background task (non-blocking)
          4. _crawl_one() will add new links back to the queue
          5. Repeat until queue is empty or max_pages reached

        WHY asyncio.create_task() instead of await?
          'await _crawl_one(url)' would process ONE URL and wait for it
          to finish before picking up the next one. That's sequential!

          'asyncio.create_task(_crawl_one(url))' schedules the coroutine
          to run in the background. The loop immediately picks up the next
          URL from the queue, so many pages are processed concurrently.
          The semaphore inside fetch_url limits actual HTTP concurrency.
        """
        exclude_patterns = exclude_patterns or []
        include_patterns = include_patterns or []

        # Extract "example.com" from "https://example.com/page" for the domain filter
        seed_domain = urlparse(start_urls[0]).netloc if start_urls else ""

        self._init_crawl_components()

        # Add all seed URLs at depth 0
        for url in start_urls:
            await self._queue.add_url(url, depth=0)

        t0 = time.perf_counter()
        last_log = t0

        logger.info("Crawl started — seed=%s, max_pages=%d, max_depth=%d",
                    seed_domain, max_pages, self.max_depth)

        while True:
            # Hard cap: stop when we've visited enough pages
            if len(self.visited_urls) >= max_pages:
                logger.info("Reached max_pages=%d, stopping.", max_pages)
                break

            item = await self._queue.get_next()

            if item is None:
                # Queue looks empty. But background tasks might be adding new
                # URLs right now. Check pending count to decide: truly done?
                if self._queue.pending_count() == 0:
                    break                    # nothing left anywhere, we're done
                await asyncio.sleep(0.1)     # brief pause, then check again
                continue

            url, depth = item

            # Launch this URL's processing as a background task.
            # The loop continues immediately to grab the next URL.
            asyncio.create_task(
                self._crawl_one(
                    url, depth, seed_domain,
                    same_domain_only, exclude_patterns, include_patterns,
                )
            )

            # Print progress every 5 seconds (not on every URL — that would spam)
            now = time.perf_counter()
            if now - last_log >= 5:
                stats = self._queue.get_stats()
                elapsed = now - t0
                speed = len(self.visited_urls) / elapsed if elapsed else 0
                logger.info(
                    "Progress — visited=%d | queued=%d | failed=%d | %.1f pages/s",
                    stats["visited"], stats["pending"], stats["failed"], speed,
                )
                last_log = now

        # The loop has exited, but some _crawl_one() tasks may still be running.
        # Wait 1.5 seconds to let them finish and write their results.
        await asyncio.sleep(1.5)

        elapsed = time.perf_counter() - t0
        logger.info(
            "Crawl done — %d pages in %.1fs (%.1f p/s) | %d failed",
            len(self.visited_urls), elapsed,
            len(self.visited_urls) / elapsed if elapsed else 0,
            len(self.failed_urls),
        )
        return self.processed_urls

    def get_crawl_stats(self) -> dict:
        """Return combined statistics from queue, rate limiter, and robots parser."""
        stats = {}
        if self._queue:
            stats["queue"] = self._queue.get_stats()
        if self._rate_limiter:
            stats["rate_limiter"] = self._rate_limiter.get_stats()
        if self._robots_parser:
            stats["robots"] = self._robots_parser.get_stats()
        return stats

    # ================================================================== #
    #  Shutdown and context-manager support                               #
    # ================================================================== #

    async def close(self) -> None:
        """
        Properly shut down the aiohttp session.

        WHY is this important?
          aiohttp keeps TCP connections open for reuse (the connection pool).
          If you don't call close(), Python will warn "Unclosed client session"
          and those connections stay open until the OS eventually cleans them up.
          Always call close() or use 'async with AsyncCrawler() as c:'.
        """
        await self._session.close()
        # One event loop tick to let aiohttp fully close SSL sockets
        await asyncio.sleep(0)
        logger.info("AsyncCrawler session closed.")

    async def __aenter__(self) -> "AsyncCrawler":
        # Enables:  async with AsyncCrawler() as crawler:
        return self

    async def __aexit__(self, *_) -> None:
        # Called automatically at the end of 'async with' block
        await self.close()
