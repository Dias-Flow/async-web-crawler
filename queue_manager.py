"""
async_crawler/queue_manager.py  (Day 3)

Two classes that handle WHO gets processed WHEN and HOW MANY at once.

CrawlerQueue:
  The "to-do list" for the crawler. Knows about every URL ever seen,
  its current state (waiting / in-progress / done / failed),
  and which URLs to hand out next.

SemaphoreManager:
  A "traffic controller" with two levels:
    Global level:  only N requests running total across ALL domains.
    Domain level:  only M requests running to ONE specific domain.
"""

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger("CrawlerQueue")


# ===========================================================================
# URLRecord — internal state for one URL
# ===========================================================================
@dataclass
class URLRecord:
    """
    Everything the queue knows about one URL.

    State machine (one-way transitions):
      pending → processing → visited   (happy path)
      pending → processing → failed    (error path)

    WHY track state?
      Without state tracking, we might process the same URL twice if two
      concurrent workers both pick it up. The 'processing' state acts as a lock.

    WHY track depth?
      Depth = how many link-hops from the seed URL.
      Seed is depth 0. Its direct links are depth 1. Their links are depth 2.
      We use depth to enforce max_depth and stop the crawl from going forever.
    """
    url: str
    priority: int = 0          # lower number = picked up sooner
    depth: int = 0             # hops from seed URL
    added_at: float = field(default_factory=time.time)
    state: str = "pending"     # pending | processing | visited | failed
    error: Optional[str] = None


# ===========================================================================
# CrawlerQueue
# ===========================================================================
class CrawlerQueue:
    """
    A priority queue that prevents duplicate URLs and tracks crawl state.

    WHY NOT just use asyncio.PriorityQueue directly?
      asyncio.PriorityQueue only gives FIFO with priority — it has no idea
      whether a URL has been processed or even if you added the same URL twice.
      We need:
        - O(1) duplicate detection (_index dict lookup)
        - State tracking per URL
        - Statistics (how many visited, failed, pending?)
      So we wrap PriorityQueue and add an index dict alongside it.

    INTERNAL STRUCTURE:
      _queue: asyncio.PriorityQueue
        Stores tuples: (priority, counter, url, depth)
        Priority queue pops the SMALLEST tuple first.
        'counter' is a tiebreaker so same-priority URLs come out FIFO.

      _index: dict[url → URLRecord]
        Fast lookup to check "have we seen this URL before?"
        Also the source of truth for state.
    """

    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._index: dict[str, URLRecord] = {}   # url → record
        self._counter: int = 0                    # ever-increasing, never resets
        self._lock = asyncio.Lock()               # protects _index from race conditions

    async def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        """
        Add a URL to the queue. Returns True if added, False if duplicate.

        The asyncio.Lock here is critical for correctness.
        WHY? Imagine two coroutines both check "is 'example.com' in _index?"
        at the same moment:
          Coroutine A: checks → not in index → plans to add
          Coroutine B: checks → not in index → plans to add
          Coroutine A: adds it
          Coroutine B: adds it AGAIN (duplicate!)

        The lock prevents this: only one coroutine can be inside the
        'async with self._lock' block at a time.
        """
        async with self._lock:
            if url in self._index:
                return False  # already known — skip silently

            record = URLRecord(url=url, priority=priority, depth=depth)
            self._index[url] = record
            self._counter += 1

            # PriorityQueue sorts by the first element of the tuple (priority).
            # Counter as second element means equal-priority items are FIFO.
            await self._queue.put((priority, self._counter, url, depth))
            logger.debug("Queued [depth=%d pri=%d] %s", depth, priority, url)
            return True

    async def get_next(self) -> Optional[tuple[str, int]]:
        """
        Pop and return the next (url, depth) to process.
        Returns None immediately if queue is empty (non-blocking).

        WHY non-blocking (get_nowait) instead of blocking (get)?
          The crawl loop needs to check other conditions (max_pages, shutdown)
          between each URL. A blocking get() would freeze the loop.
          Instead, the loop calls get_next(), gets None if empty,
          sleeps briefly, and checks again.
        """
        try:
            _, _, url, depth = self._queue.get_nowait()
            async with self._lock:
                if url in self._index:
                    self._index[url].state = "processing"
            return url, depth
        except asyncio.QueueEmpty:
            return None  # queue is empty right now — caller decides what to do

    def mark_visited(self, url: str) -> None:
        """
        Mark URL as successfully processed.

        IMPORTANT: task_done() must be called once for every get_nowait().
        This is required by asyncio.PriorityQueue's internal bookkeeping.
        Without it, queue.join() (if used) would never unblock.
        """
        if url in self._index:
            self._index[url].state = "visited"
        self._queue.task_done()

    def mark_failed(self, url: str, error: str) -> None:
        """Mark URL as permanently failed with an error reason."""
        if url in self._index:
            self._index[url].state = "failed"
            self._index[url].error = error
        self._queue.task_done()

    def is_known(self, url: str) -> bool:
        """True if this URL exists in the index in ANY state."""
        return url in self._index

    def pending_count(self) -> int:
        """Approximate number of items still waiting (not processing, not done)."""
        return self._queue.qsize()

    def get_stats(self) -> dict:
        """
        Count URLs by state. Used for the real-time progress display.

        defaultdict(int) starts every key at 0 automatically,
        so counts["visited"] is 0 even if no URLs are visited yet.
        """
        counts: dict[str, int] = defaultdict(int)
        for rec in self._index.values():
            counts[rec.state] += 1
        return {
            "total_seen": len(self._index),
            # pending + processing both mean "not done yet"
            "pending":    counts["pending"] + counts["processing"],
            "visited":    counts["visited"],
            "failed":     counts["failed"],
        }

    def get_failed(self) -> dict[str, str]:
        """Return {url: error_message} for all failed URLs. Useful for reports."""
        return {
            rec.url: rec.error or "unknown"
            for rec in self._index.values()
            if rec.state == "failed"
        }


# ===========================================================================
# SemaphoreManager
# ===========================================================================
class SemaphoreManager:
    """
    Two-level concurrency control.

    WHY two levels?
      Imagine max_concurrent=10 and you're crawling a site with 1000 pages.
      Without per-domain limits, all 10 slots could be used by the same domain,
      effectively DDoS-ing that server. Per-domain limits say "max 3 requests
      to example.com at once, regardless of how many global slots are free."

    LEVEL 1 — global semaphore:
      Total requests across ALL domains at once. The same gate as in Day 1
      AsyncCrawler, but now managed here so both levels can be acquired together.

    LEVEL 2 — per-domain semaphore:
      One semaphore per hostname. Created on first access via defaultdict-like logic.
      example.com gets its own semaphore, httpbin.org gets its own semaphore.
      They don't interfere with each other.

    USAGE:
      async with await sem_manager.domain_context(url):
          result = await fetch(url)
      # Both semaphores released automatically on exit
    """

    def __init__(self, global_limit: int = 10, per_domain_limit: int = 3) -> None:
        self._global = asyncio.Semaphore(global_limit)
        self._per_domain_limit = per_domain_limit
        self._domain_sems: dict[str, asyncio.Semaphore] = {}
        self._lock = asyncio.Lock()  # protects _domain_sems dict creation

    def _extract_domain(self, url: str) -> str:
        """Extract just 'example.com' from 'https://example.com/page?q=1'."""
        return urlparse(url).netloc

    async def _get_domain_sem(self, domain: str) -> asyncio.Semaphore:
        """
        Return the semaphore for a domain, creating it on first access.

        WHY a lock here?
          Two coroutines hitting a new domain simultaneously could both
          try to create a semaphore for it. The lock ensures only one wins.
          After creation, the semaphore is in the dict and everyone reuses it.
        """
        async with self._lock:
            if domain not in self._domain_sems:
                self._domain_sems[domain] = asyncio.Semaphore(self._per_domain_limit)
            return self._domain_sems[domain]

    class _DomainContext:
        """
        An async context manager that holds BOTH semaphores simultaneously.

        WHY acquire global FIRST, then domain?
          This ordering prevents a deadlock scenario:
            If coroutine A holds global and waits for domain,
            AND coroutine B holds domain and waits for global,
            they'd wait forever. Consistent ordering breaks the cycle.
          (This is the classic "acquire locks in fixed order" pattern.)
        """
        def __init__(self, global_sem: asyncio.Semaphore, domain_sem: asyncio.Semaphore):
            self._g = global_sem
            self._d = domain_sem

        async def __aenter__(self):
            await self._g.acquire()  # grab global slot first
            await self._d.acquire()  # then grab domain slot
            return self

        async def __aexit__(self, *_):
            self._d.release()  # release in reverse order (domain first)
            self._g.release()

    async def domain_context(self, url: str) -> "_DomainContext":
        """
        Return a context manager for the given URL's domain.

        Usage:
            ctx = await sem_manager.domain_context("https://example.com/page")
            async with ctx:
                await fetch(url)
        """
        domain = self._extract_domain(url)
        domain_sem = await self._get_domain_sem(domain)
        return self._DomainContext(self._global, domain_sem)

    def get_active_domains(self) -> list[str]:
        """List all domains that have been seen at least once."""
        return list(self._domain_sems.keys())
