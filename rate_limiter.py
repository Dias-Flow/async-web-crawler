import aiohttp
"""
async_crawler/rate_limiter.py  (Day 4)

Two classes that make the crawler "polite":

RateLimiter:
  Enforces a minimum time gap between requests to the same domain.
  Without this, the crawler would fire 100 requests per second and
  likely get IP-banned or crash a small server.

RobotsParser:
  Downloads and reads robots.txt — a file that site owners use to tell
  crawlers which pages they are NOT allowed to visit.
  Ignoring robots.txt is considered rude and sometimes illegal.
"""

import asyncio
import logging
import random
import time
from typing import Optional
from urllib.parse import urlparse, urljoin
from urllib.robotparser import RobotFileParser

logger = logging.getLogger("RateLimiter")


# ===========================================================================
# RateLimiter
# ===========================================================================
class RateLimiter:
    """
    Enforces a minimum delay between HTTP requests to each domain.

    HOW IT WORKS — "last request timestamp" pattern:
      Before every request we check: when was the last request to this domain?
      If it was less than (1 / requests_per_second) seconds ago, we sleep
      for the remaining time. If enough time has passed, we proceed immediately.

      Example with requests_per_second=2.0 (one request every 0.5s):
        t=0.00s  Request 1 to example.com → no wait, proceed
        t=0.10s  Request 2 to example.com → wait 0.40s (need 0.5s gap)
        t=0.50s  Request 3 to example.com → no wait (0.40s passed since last)
        t=0.51s  Request 4 to httpbin.org → no wait (different domain!)

    WHY per-domain and not global?
      With per_domain=True, example.com and httpbin.org each have their
      own timer. Waiting for example.com does NOT delay httpbin.org.
      This gives us full politeness without sacrificing parallelism.

    JITTER:
      Adding a small random delay (0–jitter seconds) makes requests look
      less robotic. Servers that detect uniform 1.000s intervals may flag
      you as a bot. Random intervals between 0.8s and 1.3s look more human.
    """

    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
    ) -> None:
        """
        Args:
            requests_per_second: Target rate. 2.0 = max 2 requests/sec per domain.
            per_domain:          True = each domain has its own timer.
                                 False = one global timer for all domains.
            min_delay:           Hard minimum gap regardless of requests_per_second.
                                 Useful when a site's robots.txt says Crawl-delay: 2.
            jitter:              Max extra random seconds added to every delay.
        """
        # Convert rate to interval: 2 req/s → 0.5s between requests
        # max(..., 0.001) prevents division by zero if someone passes 0
        self._interval = 1.0 / max(requests_per_second, 0.001)
        self._per_domain = per_domain
        self._min_delay = min_delay
        self._jitter = jitter

        # Maps "domain key" → timestamp of last request to that domain
        # "_global" is the key when per_domain=False
        self._last_request: dict[str, float] = {}

        # One asyncio.Lock per domain so two coroutines hitting the same
        # domain don't both calculate "no wait needed" simultaneously
        self._locks: dict[str, asyncio.Lock] = {}
        self._meta_lock = asyncio.Lock()  # protects _locks dict creation

        # Cumulative stats
        self._total_waits: int = 0
        self._total_wait_time: float = 0.0

    async def _get_lock(self, key: str) -> asyncio.Lock:
        """Get (or create) the per-domain lock. Protected by meta_lock."""
        async with self._meta_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]

    def _key(self, url: str) -> str:
        """
        Determine the bucket key for this URL.
        With per_domain=True:  "example.com", "httpbin.org", etc.
        With per_domain=False: "_global" (everything shares one timer)
        """
        if not self._per_domain:
            return "_global"
        return urlparse(url).netloc or "_global"

    async def acquire(self, url: str = "") -> float:
        """
        Wait until it is polite to make a request to this URL's domain.
        Returns how many seconds we actually slept (0 if no wait was needed).

        Call this BEFORE every HTTP request in the crawl loop.

        The per-domain lock ensures that two coroutines both heading to
        example.com don't both see "no wait needed" and fire simultaneously.
        One of them will get the lock, sleep, update the timestamp, and release.
        The other then gets the lock, sees the updated timestamp, and waits its turn.
        """
        key = self._key(url)
        lock = await self._get_lock(key)

        async with lock:
            now = time.monotonic()
            last = self._last_request.get(key, 0.0)

            # How many seconds since our last request to this domain?
            elapsed_since_last = now - last

            # Required minimum gap = max of rate-based interval and hard min_delay
            required_gap = max(self._interval, self._min_delay)

            # How much longer do we need to wait?
            wait = max(0.0, required_gap - elapsed_since_last)

            # Add random jitter on top (makes timing less predictable)
            if self._jitter > 0:
                wait += random.uniform(0, self._jitter)

            if wait > 0:
                logger.debug("Rate limit: sleeping %.2fs for %s", wait, key)
                self._total_waits += 1
                self._total_wait_time += wait
                await asyncio.sleep(wait)

            # Record WHEN we sent this request (after the sleep)
            self._last_request[key] = time.monotonic()
            return wait

    def get_stats(self) -> dict:
        return {
            "total_waits":     self._total_waits,
            "total_wait_time": round(self._total_wait_time, 3),
            "avg_wait":        round(
                self._total_wait_time / self._total_waits, 3
            ) if self._total_waits else 0.0,
            "domains_tracked": len([k for k in self._last_request if k != "_global"]),
        }


# ===========================================================================
# RobotsParser
# ===========================================================================
class RobotsParser:
    """
    Downloads, parses, and caches robots.txt for each domain.

    WHAT IS robots.txt?
      A plain-text file at https://example.com/robots.txt that tells crawlers:
        User-agent: *          ← rules for all bots
        Disallow: /private/    ← don't crawl /private/ or anything under it
        Disallow: /admin/      ← don't crawl /admin/
        Crawl-delay: 2         ← wait 2 seconds between requests

      Respecting robots.txt is a standard courtesy.
      Some sites also check legally that crawlers obey it.

    WHY CACHE?
      robots.txt is one file per domain. If we're crawling 500 pages of
      example.com, we should fetch robots.txt ONCE and cache the result.
      Fetching it before every single page would add 500 extra requests.

    STDLIB robotparser:
      Python's urllib.robotparser.RobotFileParser does the actual parsing
      (handling wildcards, Allow/Disallow ordering, etc.).
      We just need to fetch the file asynchronously and feed it the text.
    """

    def __init__(self, user_agent: str = "*") -> None:
        """
        Args:
            user_agent: Which User-Agent rules to check against.
                        Use "*" to match the catch-all wildcard rules.
                        Use "MyBot" to check rules specifically for "MyBot".
        """
        self._user_agent = user_agent
        # Maps "https://example.com" → RobotFileParser instance
        self._cache: dict[str, RobotFileParser] = {}
        self._blocked_count: int = 0

    async def fetch_robots(self, session, base_url: str) -> RobotFileParser:
        """
        Fetch and parse robots.txt for the site at base_url.

        Returns a RobotFileParser object.
        On subsequent calls for the same domain, returns the cached object
        without making any HTTP request.

        Args:
            session:  An open aiohttp.ClientSession to reuse for the request.
            base_url: Any URL on the target site — we extract scheme+host from it.
        """
        # Extract just "https://example.com" from "https://example.com/page?q=1"
        parsed = urlparse(base_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"

        # Cache hit → return immediately, no network request
        if origin in self._cache:
            return self._cache[origin]

        robots_url = urljoin(origin, "/robots.txt")
        rfp = RobotFileParser()
        rfp.set_url(robots_url)

        try:
            async with session.get(robots_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    # rfp.parse() expects a list of lines, not one big string
                    rfp.parse(text.splitlines())
                    logger.info("robots.txt fetched for %s", origin)
                else:
                    # 404 is common — site has no robots.txt → allow everything.
                    # We must explicitly parse an empty allow-all rule because
                    # an un-parsed RobotFileParser blocks everything by default
                    # (it treats the file as "unavailable" = disallow all).
                    rfp.parse(["User-agent: *", "Allow: /"])
                    logger.debug("No robots.txt at %s (HTTP %d), allowing all", robots_url, resp.status)

        except Exception as exc:
            # Network failure fetching robots.txt → allow everything
            # Must explicitly set allow-all because un-parsed rfp blocks by default
            rfp.parse(["User-agent: *", "Allow: /"])
            logger.warning("Could not fetch robots.txt for %s: %s (allowing all)", origin, exc)

        # Cache even on failure so we don't retry on every subsequent page
        self._cache[origin] = rfp
        return rfp

    def can_fetch(self, rfp: RobotFileParser, url: str) -> bool:
        """
        Check whether our user-agent is allowed to fetch this URL.

        Uses Python's RobotFileParser which handles:
          - Wildcards: "Disallow: /*.pdf$"
          - Allow overrides: "Allow: /public/" takes priority over "Disallow: /"
          - User-agent matching

        Returns True (allowed) or False (blocked).
        Blocked URLs are counted for statistics.
        """
        allowed = rfp.can_fetch(self._user_agent, url)
        if not allowed:
            self._blocked_count += 1
            logger.info("robots.txt BLOCKED: %s", url)
        return allowed

    def get_crawl_delay(self, rfp: RobotFileParser) -> Optional[float]:
        """
        Read the Crawl-delay directive for our user agent.

        Some sites specify: "Crawl-delay: 5" meaning "wait 5 seconds between requests".
        We pass this to asyncio.sleep() in _crawl_one() to honour it.
        Returns None if no Crawl-delay is specified.
        """
        return rfp.crawl_delay(self._user_agent)

    def get_stats(self) -> dict:
        return {
            "domains_cached": len(self._cache),
            "total_blocked":  self._blocked_count,
        }
