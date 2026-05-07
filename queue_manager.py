"""
async_crawler/queue_manager.py  (Day 3)

FIX: get_next() now returns str|None per Day-3 course spec.
     Depth is tracked inside URLRecord and returned separately via get_depth().
     The crawl loop in crawler.py calls get_next() → str, then get_depth(url) → int.
"""

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger("CrawlerQueue")


@dataclass
class URLRecord:
    url: str
    priority: int = 0
    depth: int = 0
    added_at: float = field(default_factory=time.time)
    state: str = "pending"
    error: Optional[str] = None


class CrawlerQueue:
    """
    Priority queue for URLs with duplicate detection and state tracking.

    FIX: get_next() returns str|None (just the URL) per Day-3 spec.
         Use get_depth(url) to retrieve depth after get_next() returns.
    """

    def __init__(self) -> None:
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._index: dict[str, URLRecord] = {}
        self._counter: int = 0
        self._lock = asyncio.Lock()

    async def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        """
        Enqueue a URL. Returns True if new, False if already known.
        Depth is stored in URLRecord and retrievable via get_depth().
        """
        async with self._lock:
            if url in self._index:
                return False
            record = URLRecord(url=url, priority=priority, depth=depth)
            self._index[url] = record
            self._counter += 1
            await self._queue.put((priority, self._counter, url, depth))
            logger.debug("Queued [depth=%d pri=%d] %s", depth, priority, url)
            return True

    async def get_next(self) -> Optional[str]:
        """
        Pop and return the next URL string, or None if queue is empty.

        Per Day-3 spec: returns str|None (not a tuple).
        To get the depth after calling this, use get_depth(url).

        WHY return str and not (str, int)?
          The course spec defines get_next() -> str|None.
          Depth is secondary data; get_depth() provides it on demand.
        """
        try:
            _, _, url, depth = self._queue.get_nowait()
            async with self._lock:
                if url in self._index:
                    self._index[url].state = "processing"
            return url          # ← str only, per spec
        except asyncio.QueueEmpty:
            return None

    def get_depth(self, url: str) -> int:
        """
        Return the depth of a URL (how many hops from seed).
        Call this after get_next() returns the URL string.
        Returns 0 if URL is not in the index (safe fallback).
        """
        rec = self._index.get(url)
        return rec.depth if rec else 0

    def mark_processed(self, url: str, error: str = None) -> None:
        """
        Mark a URL as done. Per Day-3 spec the method is called mark_processed().
        If error is provided the URL is marked as "failed", otherwise "visited".
        task_done() is required by asyncio.PriorityQueue bookkeeping.
        """
        if url in self._index:
            if error:
                self._index[url].state = "failed"
                self._index[url].error = error
            else:
                self._index[url].state = "visited"
        self._queue.task_done()

    def mark_visited(self, url: str) -> None:
        """Alias for mark_processed() kept for backward compatibility."""
        self.mark_processed(url)

    def mark_failed(self, url: str, error: str) -> None:
        if url in self._index:
            self._index[url].state = "failed"
            self._index[url].error = error
        self._queue.task_done()

    def is_known(self, url: str) -> bool:
        return url in self._index

    def pending_count(self) -> int:
        return self._queue.qsize()

    def get_stats(self) -> dict:
        counts: dict[str, int] = defaultdict(int)
        for rec in self._index.values():
            counts[rec.state] += 1
        return {
            "total_seen": len(self._index),
            "pending":    counts["pending"] + counts["processing"],
            "visited":    counts["visited"],
            "failed":     counts["failed"],
        }

    def get_failed(self) -> dict[str, str]:
        return {
            rec.url: rec.error or "unknown"
            for rec in self._index.values()
            if rec.state == "failed"
        }


class SemaphoreManager:
    """Two-level concurrency: global cap + per-domain cap. Unchanged."""

    def __init__(self, global_limit: int = 10, per_domain_limit: int = 3) -> None:
        self._global = asyncio.Semaphore(global_limit)
        self._per_domain_limit = per_domain_limit
        self._domain_sems: dict[str, asyncio.Semaphore] = {}
        self._lock = asyncio.Lock()

    def _extract_domain(self, url: str) -> str:
        return urlparse(url).netloc

    async def _get_domain_sem(self, domain: str) -> asyncio.Semaphore:
        async with self._lock:
            if domain not in self._domain_sems:
                self._domain_sems[domain] = asyncio.Semaphore(self._per_domain_limit)
            return self._domain_sems[domain]

    class _DomainContext:
        def __init__(self, global_sem, domain_sem):
            self._g = global_sem
            self._d = domain_sem
        async def __aenter__(self):
            await self._g.acquire()
            await self._d.acquire()
            return self
        async def __aexit__(self, *_):
            self._d.release()
            self._g.release()

    async def domain_context(self, url: str):
        domain = self._extract_domain(url)
        domain_sem = await self._get_domain_sem(domain)
        return self._DomainContext(self._global, domain_sem)

    def get_active_domains(self) -> list[str]:
        return list(self._domain_sems.keys())
