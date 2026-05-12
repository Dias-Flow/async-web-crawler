"""
async_crawler/rate_limiter.py  (Day 4)

FIXES applied (mentor review):
  1. RateLimiter.acquire(domain=None) — per spec; accepts plain domain
     ("example.com"), full URL ("https://example.com/page"), or None (global).
  2. RobotsParser — matches spec API exactly:
       fetch_robots(base_url: str) -> dict   (no external session param)
       can_fetch(url: str, user_agent: str = "*") -> bool
       get_crawl_delay(user_agent: str = "*") -> Optional[float]
     Manages own aiohttp.ClientSession internally; caches rules per domain.
"""

import aiohttp
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
    Enforces a minimum delay between HTTP requests per domain.

    FIX: acquire() now accepts domain: str | None per Day-4 spec.
      - None          → global bucket ("_global")
      - "example.com" → per-domain bucket (plain domain, no scheme)
      - "https://example.com/page" → extracts netloc automatically

    WHY this matters:
      Old signature was acquire(url: str = ""). Passing "example.com" (no scheme)
      to urlparse gives netloc="" → everything fell into the "_global" bucket,
      breaking per-domain rate limiting entirely.
    """

    def __init__(
        self,
        requests_per_second: float = 1.0,
        per_domain: bool = True,
        min_delay: float = 0.0,
        jitter: float = 0.0,
    ) -> None:
        self._interval = 1.0 / max(requests_per_second, 0.001)
        self._per_domain = per_domain
        self._min_delay = min_delay
        self._jitter = jitter
        self._last_request: dict[str, float] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._meta_lock = asyncio.Lock()
        self._total_waits: int = 0
        self._total_wait_time: float = 0.0

    async def _get_lock(self, key: str) -> asyncio.Lock:
        async with self._meta_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            return self._locks[key]

    def _key(self, domain: Optional[str]) -> str:
        """
        Convert the domain argument to an internal bucket key.

        Handles three input forms:
          None                        → "_global"
          "example.com"              → "example.com"   (plain domain, no scheme)
          "https://example.com/path" → "example.com"   (URL, extract netloc)

        The old code used urlparse(url).netloc which returns "" for plain
        domains (no "://"), collapsing them all into "_global".
        """
        if not self._per_domain or domain is None:
            return "_global"
        # If it looks like a full URL (has "://"), extract netloc
        if "://" in domain:
            netloc = urlparse(domain).netloc
            return netloc or "_global"
        # Plain domain string like "example.com" or "example.com:8080"
        # Strip path/query if accidentally included
        return domain.split("/")[0] or "_global"

    async def acquire(self, domain: Optional[str] = None) -> float:
        """
        Wait until it is polite to send the next request.

        Args:
            domain: Target domain ("example.com"), full URL, or None for global.
                    Per Day-4 spec: acquire(self, domain: str | None).

        Returns:
            Seconds actually slept (0.0 if no wait was needed).
        """
        key = self._key(domain)
        lock = await self._get_lock(key)

        async with lock:
            now = time.monotonic()
            last = self._last_request.get(key, 0.0)
            elapsed_since_last = now - last
            required_gap = max(self._interval, self._min_delay)
            wait = max(0.0, required_gap - elapsed_since_last)
            if self._jitter > 0:
                wait += random.uniform(0, self._jitter)
            if wait > 0:
                logger.debug("Rate limit: sleeping %.2fs for %s", wait, key)
                self._total_waits += 1
                self._total_wait_time += wait
                await asyncio.sleep(wait)
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
    Downloads, parses, and caches robots.txt per domain.

    FIX: API now matches Day-4 spec exactly.

    OLD (broken) API:
        rfp = await parser.fetch_robots(session, base_url)   # external session
        parser.can_fetch(rfp, url)                           # rfp passed manually
        parser.get_crawl_delay(rfp)                          # rfp passed manually

    NEW (spec-compliant) API:
        await parser.fetch_robots(base_url)                  # manages own session
        parser.can_fetch(url, user_agent="*")                # no rfp needed
        parser.get_crawl_delay(user_agent="*")               # no rfp needed

    WHY own session?
      The spec signature fetch_robots(self, base_url: str) has no session param.
      RobotsParser creates and owns an aiohttp.ClientSession, reusing it across
      all fetch_robots() calls. Call close() when done to release it.

    WHY store _last_domain?
      get_crawl_delay() has no URL/domain param per spec.  It returns the delay
      for the most-recently checked domain (i.e., the domain of the last
      fetch_robots() or can_fetch() call).  This matches the expected usage:
          await robots.fetch_robots(url)
          if not robots.can_fetch(url): skip
          delay = robots.get_crawl_delay()   # delay for url's domain
    """

    def __init__(self, user_agent: str = "*") -> None:
        self._user_agent = user_agent
        # Maps "https://example.com" → RobotFileParser
        self._cache: dict[str, RobotFileParser] = {}
        self._blocked_count: int = 0
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_domain: Optional[str] = None   # for get_crawl_delay()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_session(self) -> aiohttp.ClientSession:
        """Return the internal session, creating it lazily."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10),
                headers={"User-Agent": self._user_agent},
            )
        return self._session

    @staticmethod
    def _extract_origin(url: str) -> str:
        """Pull 'https://example.com' from any URL on that site."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}"

    # ------------------------------------------------------------------
    # Public API — matches Day-4 spec
    # ------------------------------------------------------------------

    async def fetch_robots(self, base_url: str) -> dict:
        """
        Fetch and cache robots.txt for the domain of base_url.

        Per spec: fetch_robots(self, base_url: str) -> dict
          Returns a summary dict (rules cached internally for can_fetch / get_crawl_delay).

        Subsequent calls for the same domain are instant (cache hit).
        On network failure or 404, allows everything (explicit allow-all parse).
        """
        origin = self._extract_origin(base_url)
        self._last_domain = origin   # remember for get_crawl_delay()

        if origin in self._cache:
            rfp = self._cache[origin]
            return {"domain": origin, "cached": True,
                    "crawl_delay": rfp.crawl_delay(self._user_agent)}

        robots_url = urljoin(origin, "/robots.txt")
        rfp = RobotFileParser()
        rfp.set_url(robots_url)

        session = await self._get_session()
        try:
            async with session.get(robots_url) as resp:
                if resp.status == 200:
                    text = await resp.text()
                    rfp.parse(text.splitlines())
                    logger.info("robots.txt fetched for %s", origin)
                else:
                    # 404 / other: no robots.txt → allow all
                    rfp.parse(["User-agent: *", "Allow: /"])
                    logger.debug("No robots.txt at %s (HTTP %d), allowing all",
                                 robots_url, resp.status)
        except Exception as exc:
            rfp.parse(["User-agent: *", "Allow: /"])
            logger.warning("robots.txt fetch failed for %s: %s (allowing all)", origin, exc)

        self._cache[origin] = rfp
        return {
            "domain":      origin,
            "cached":      False,
            "crawl_delay": rfp.crawl_delay(self._user_agent),
        }

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        """
        Check whether the given URL is allowed per cached robots.txt rules.

        Per spec: can_fetch(self, url: str, user_agent: str = "*") -> bool

        Extracts the domain from url, looks up the cached RobotFileParser.
        Returns True (allow all) if robots.txt was never fetched for this domain.
        """
        origin = self._extract_origin(url)
        self._last_domain = origin   # keep in sync

        rfp = self._cache.get(origin)
        if rfp is None:
            # Never fetched → default allow
            return True

        ua = user_agent if user_agent != "*" else self._user_agent
        allowed = rfp.can_fetch(ua, url)
        if not allowed:
            self._blocked_count += 1
            logger.info("robots.txt BLOCKED: %s", url)
        return allowed

    def get_crawl_delay(self, user_agent: str = "*") -> Optional[float]:
        """
        Return the Crawl-delay for the most recently checked domain.

        Per spec: get_crawl_delay(self, user_agent: str = "*") -> float

        Always call fetch_robots() or can_fetch() first so _last_domain is set.
        """
        if not self._last_domain:
            return None
        rfp = self._cache.get(self._last_domain)
        if rfp is None:
            return None
        ua = user_agent if user_agent != "*" else self._user_agent
        return rfp.crawl_delay(ua)

    def get_stats(self) -> dict:
        return {
            "domains_cached": len(self._cache),
            "total_blocked":  self._blocked_count,
        }

    async def close(self) -> None:
        """Close the internal aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None
