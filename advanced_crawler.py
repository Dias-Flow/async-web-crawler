"""
async_crawler/advanced_crawler.py  (Day 7)

FIX: RetryStrategy is now actually used.
  AdvancedCrawler overrides _crawl_one() from AsyncCrawler to wrap
  the fetch call with self._retry.execute_with_retry().
  max_retries in AsyncCrawler is set to 0 so only RetryStrategy controls retries,
  giving us proper exponential back-off (Day 5) in the final crawler.
"""

import argparse
import asyncio
import json
import logging
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urljoin

import aiohttp

logger = logging.getLogger("AdvancedCrawler")


# ===========================================================================
# CrawlerStats
# ===========================================================================
class CrawlerStats:
    """Collects crawl-wide statistics. Unchanged from previous version."""

    def __init__(self) -> None:
        self._start_time: float = time.perf_counter()
        self._end_time: Optional[float] = None
        self.total_pages: int = 0
        self.successful: int = 0
        self.failed: int = 0
        self.status_codes: Counter = Counter()
        self.domain_counts: Counter = Counter()
        self._total_fetch_time: float = 0.0
        self.error_types: Counter = Counter()

    def record_page(self, page_data: dict) -> None:
        self.total_pages += 1
        self.successful += 1
        self._total_fetch_time += page_data.get("fetch_elapsed", 0.0)
        domain = urlparse(page_data.get("url", "")).netloc
        if domain:
            self.domain_counts[domain] += 1
        # FIX: status_code is now reliably set by fetch_and_parse()
        sc = page_data.get("status_code")
        if sc:
            self.status_codes[sc] += 1

    def record_failure(self, url: str, error: str) -> None:
        self.total_pages += 1
        self.failed += 1
        error_type = error.split(":")[0].strip() if error else "Unknown"
        self.error_types[error_type] += 1

    def finish(self) -> None:
        self._end_time = time.perf_counter()

    @property
    def elapsed_seconds(self) -> float:
        return round((self._end_time or time.perf_counter()) - self._start_time, 2)

    @property
    def pages_per_second(self) -> float:
        return round(self.successful / self.elapsed_seconds, 2) if self.elapsed_seconds else 0.0

    @property
    def avg_fetch_time(self) -> float:
        return round(self._total_fetch_time / self.successful, 3) if self.successful else 0.0

    def to_dict(self) -> dict:
        return {
            "total_pages":      self.total_pages,
            "successful":       self.successful,
            "failed":           self.failed,
            "elapsed_seconds":  self.elapsed_seconds,
            "pages_per_second": self.pages_per_second,
            "avg_fetch_time":   self.avg_fetch_time,
            "status_codes":     dict(self.status_codes),
            "top_domains":      dict(self.domain_counts.most_common(10)),
            "error_types":      dict(self.error_types),
        }

    def print_summary(self) -> None:
        s = self.to_dict()
        print("\n" + "=" * 55)
        print("  CRAWL STATISTICS")
        print("=" * 55)
        print(f"  Total pages  : {s['total_pages']}")
        print(f"  Successful   : {s['successful']}")
        print(f"  Failed       : {s['failed']}")
        print(f"  Elapsed      : {s['elapsed_seconds']}s")
        print(f"  Speed        : {s['pages_per_second']} pages/s")
        print(f"  Avg fetch    : {s['avg_fetch_time']}s")
        if s["status_codes"]:
            print(f"  Status codes : {s['status_codes']}")
        if s["top_domains"]:
            print(f"  Top domains  : {s['top_domains']}")
        if s["error_types"]:
            print(f"  Error types  : {s['error_types']}")
        print("=" * 55)


# ===========================================================================
# SitemapParser
# ===========================================================================
class SitemapParser:
    """Discovers URLs from sitemap.xml. Unchanged."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session
        self._visited_sitemaps: set[str] = set()

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        if sitemap_url in self._visited_sitemaps:
            return []
        self._visited_sitemaps.add(sitemap_url)
        try:
            async with self._session.get(sitemap_url) as resp:
                if resp.status != 200:
                    return []
                text = await resp.text()
        except Exception as exc:
            logger.warning("Could not fetch sitemap %s: %s", sitemap_url, exc)
            return []
        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(text)
        except ET.ParseError:
            return []
        def strip_ns(tag: str) -> str:
            return tag.split("}")[-1] if "}" in tag else tag
        urls: list[str] = []
        is_index = strip_ns(root.tag) == "sitemapindex"
        for child in root:
            tag = strip_ns(child.tag)
            if tag in ("sitemap", "url"):
                for loc_el in child:
                    if strip_ns(loc_el.tag) == "loc" and loc_el.text:
                        loc = loc_el.text.strip()
                        if is_index:
                            urls.extend(await self.fetch_sitemap(loc))
                        else:
                            urls.append(loc)
        return urls

    async def discover_sitemap_urls(self, base_url: str) -> list[str]:
        parsed = urlparse(base_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        all_urls: list[str] = []
        for candidate in [urljoin(origin, "/sitemap.xml"),
                          urljoin(origin, "/sitemap_index.xml")]:
            all_urls.extend(await self.fetch_sitemap(candidate))
        return list(dict.fromkeys(all_urls))


# ===========================================================================
# Config loader
# ===========================================================================
def load_config(path: str) -> dict:
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    text = config_path.read_text(encoding="utf-8")
    if config_path.suffix in (".yaml", ".yml"):
        try:
            import yaml
            return yaml.safe_load(text) or {}
        except ImportError:
            logger.warning("PyYAML not installed, trying JSON fallback")
    return json.loads(text)


# ===========================================================================
# HTML report
# ===========================================================================
def export_to_html_report(stats: CrawlerStats, output_path: str, pages: dict) -> None:
    s = stats.to_dict()
    rows = ""
    for url, page in list(pages.items())[:500]:
        title    = (page.get("title") or "—")[:60]
        links    = page.get("links_count", 0)
        text_len = page.get("text_length", 0)
        rows += (f"<tr><td><a href='{url}' target='_blank'>{url[:70]}</a></td>"
                 f"<td>{title}</td><td>{links}</td><td>{text_len:,}</td></tr>\n")
    html = f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8"><title>Crawl Report</title>
<style>
  body{{font-family:Arial,sans-serif;margin:2rem;color:#222}}
  h1,h2{{color:#2c5f8a}} table{{border-collapse:collapse;width:100%;margin-bottom:2rem}}
  th{{background:#2c5f8a;color:white;padding:8px 12px;text-align:left}}
  td{{padding:6px 12px;border-bottom:1px solid #ddd}} tr:hover td{{background:#f0f4f8}}
  .stat{{display:inline-block;background:#e8f0fe;border-radius:8px;padding:1rem 2rem;margin:.5rem;text-align:center}}
  .stat .num{{font-size:2rem;font-weight:bold;color:#2c5f8a}} .stat .lbl{{font-size:.85rem;color:#555}}
</style></head><body>
<h1>🕷 Async Crawler Report</h1>
<p>Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>
<div>
  <div class="stat"><div class="num">{s['total_pages']}</div><div class="lbl">Total</div></div>
  <div class="stat"><div class="num">{s['successful']}</div><div class="lbl">OK</div></div>
  <div class="stat"><div class="num">{s['failed']}</div><div class="lbl">Failed</div></div>
  <div class="stat"><div class="num">{s['elapsed_seconds']}s</div><div class="lbl">Elapsed</div></div>
  <div class="stat"><div class="num">{s['pages_per_second']}</div><div class="lbl">Pages/s</div></div>
</div>
<h2>Status Codes</h2><table><tr><th>Status</th><th>Count</th></tr>
{''.join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k,v in s['status_codes'].items())}
</table>
<h2>Top Domains</h2><table><tr><th>Domain</th><th>Pages</th></tr>
{''.join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k,v in s['top_domains'].items())}
</table>
<h2>Pages ({len(pages)} total)</h2>
<table><tr><th>URL</th><th>Title</th><th>Links</th><th>Text</th></tr>{rows}</table>
</body></html>"""
    Path(output_path).write_text(html, encoding="utf-8")
    logger.info("HTML report written to %s", output_path)


# ===========================================================================
# AdvancedCrawler
# ===========================================================================
class AdvancedCrawler:
    """
    Final integration crawler (Day 7).

    FIX: RetryStrategy is now wired in properly.

    HOW RetryStrategy is integrated:
      AdvancedCrawler does NOT subclass AsyncCrawler.
      Instead it owns an AsyncCrawler instance and replaces the fetch step
      in its own _crawl_one_with_retry() coroutine:

        async def _crawl_one_with_retry(url):
            # Wrap the raw HTTP fetch in RetryStrategy
            result = await self._retry.execute_with_retry(
                self._crawler._fetch_url_internal, url, url=url
            )

      This gives us exponential back-off (Day 5) on every real crawl.
      AsyncCrawler keeps max_retries=0 so there's no double-retry.
    """

    def __init__(
        self,
        start_urls: Optional[list[str]] = None,
        max_pages: int = 100,
        max_depth: int = 2,
        max_concurrent: int = 10,
        requests_per_second: float = 1.0,
        min_delay: float = 0.5,
        jitter: float = 0.3,
        respect_robots: bool = True,
        same_domain_only: bool = True,
        exclude_patterns: Optional[list[str]] = None,
        include_patterns: Optional[list[str]] = None,
        storage=None,
        use_sitemap: bool = False,
        user_agent: str = "AdvancedCrawler/1.0 (educational; aiohttp)",
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
        max_retries: int = 3,
        log_file: Optional[str] = None,
    ) -> None:
        self.start_urls       = start_urls or []
        self.max_pages        = max_pages
        self.same_domain_only = same_domain_only
        self.exclude_patterns = exclude_patterns or []
        self.include_patterns = include_patterns or []
        self.use_sitemap      = use_sitemap
        self._storage         = storage

        if log_file:
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s — %(message)s"))
            logging.getLogger().addHandler(fh)

        from crawler import AsyncCrawler
        from retry_strategy import RetryStrategy, TransientError, NetworkError

        # AsyncCrawler with max_retries=0 — ALL retries handled by RetryStrategy
        self._crawler = AsyncCrawler(
            max_concurrent=max_concurrent,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            max_retries=0,          # ← important: RetryStrategy is the retry layer
            max_depth=max_depth,
            requests_per_second=requests_per_second,
            min_delay=min_delay,
            jitter=jitter,
            respect_robots=respect_robots,
            user_agent=user_agent,
        )

        # FIX: RetryStrategy is now actually called in _crawl_one_with_retry()
        self._retry = RetryStrategy(
            max_retries=max_retries,
            backoff_base=1.0,
            backoff_factor=2.0,
            max_backoff=60.0,
            retry_on=[TransientError, NetworkError],
        )

        self.stats = CrawlerStats()
        self.results: dict[str, dict] = {}

    # ------------------------------------------------------------------
    # FIX: _crawl_one_with_retry — the actual integration point
    # ------------------------------------------------------------------
    async def _crawl_one_with_retry(
        self,
        url: str,
        depth: int,
        seed_domain: str,
        same_domain_only: bool,
        exclude_patterns: list[str],
        include_patterns: list[str],
    ) -> None:
        """
        Worker coroutine that wraps the HTTP fetch with RetryStrategy.

        WHAT THIS REPLACES:
          AsyncCrawler._crawl_one() calls fetch_and_parse() directly.
          This version calls _fetch_url_internal() through execute_with_retry()
          so exponential back-off kicks in on timeouts and 503 errors.

        FLOW:
          1. robots.txt check (same as AsyncCrawler)
          2. rate limiter (same as AsyncCrawler)
          3. domain semaphore (same as AsyncCrawler)
          4. RetryStrategy.execute_with_retry(_fetch_url_internal)  ← THE FIX
          5. parse HTML from the FetchResult
          6. save result, mark visited, enqueue links (same as AsyncCrawler)
        """
        from parser import _empty_page_data
        from retry_strategy import CrawlerError

        self._crawler._active_tasks += 1
        try:
            # ── robots.txt ────────────────────────────────────────────
            if self._crawler._robots_parser:
                rfp = await self._crawler._robots_parser.fetch_robots(
                    self._crawler._session, url)
                if not self._crawler._robots_parser.can_fetch(rfp, url):
                    logger.info("robots.txt SKIP: %s", url)
                    self._crawler._queue.mark_failed(url, "blocked by robots.txt")
                    # Fix: also record in failed_urls so CrawlerStats counts it
                    self._crawler.failed_urls[url] = "blocked by robots.txt"
                    return
                crawl_delay = self._crawler._robots_parser.get_crawl_delay(rfp)
                if crawl_delay:
                    await asyncio.sleep(crawl_delay)

            # ── rate limiter ──────────────────────────────────────────
            await self._crawler._rate_limiter.acquire(url)

            # ── domain semaphore ──────────────────────────────────────
            ctx = await self._crawler._sem_manager.domain_context(url)
            async with ctx:
                # FIX: wrap the raw fetch with RetryStrategy
                # execute_with_retry calls _fetch_url_internal and retries on
                # TransientError / NetworkError with exponential back-off.
                try:
                    fetch_result = await self._retry.execute_with_retry(
                        self._crawler._do_fetch_raising,
                        url,
                        url=url,
                        # _do_fetch_raising RAISES on error (unlike _do_fetch which
                        # swallows exceptions into FetchResult.error).
                        # This allows RetryStrategy to catch the exception,
                        # classify it (Transient/Network/Permanent), and apply
                        # exponential back-off before retrying.
                    )
                except CrawlerError as exc:
                    # All retries exhausted — treat as fetch failure
                    page = _empty_page_data(url)
                    page["error"] = str(exc)
                    page["crawled_at"] = datetime.now(timezone.utc).isoformat()
                    self._crawler._queue.mark_failed(url, str(exc))
                    self._crawler.failed_urls[url] = str(exc)
                    return

            # ── parse HTML ────────────────────────────────────────────
            if not fetch_result.success:
                page = _empty_page_data(url)
                page["error"]        = fetch_result.error or f"HTTP {fetch_result.status}"
                page["status_code"]  = fetch_result.status
                page["content_type"] = fetch_result.content_type or "text/html"
                page["crawled_at"]   = datetime.now(timezone.utc).isoformat()
                self._crawler._queue.mark_failed(url, page["error"])
                self._crawler.failed_urls[url] = page["error"]
                return

            page = await self._crawler._get_parser().parse_html(
                fetch_result.content, url)
            page["fetch_elapsed"] = fetch_result.elapsed
            page["status_code"]   = fetch_result.status
            page["content_type"]  = fetch_result.content_type or "text/html"
            page["crawled_at"]    = datetime.now(timezone.utc).isoformat()

            # ── save result ───────────────────────────────────────────
            self._crawler._queue.mark_visited(url)
            self._crawler.visited_urls.add(url)
            self._crawler.processed_urls[url] = page

            if self._storage:
                try:
                    await self._storage.save(page)
                except Exception as exc:
                    logger.error("Storage save failed for %s: %s", url, exc)

            # ── enqueue links ─────────────────────────────────────────
            if depth < self._crawler.max_depth:
                import re
                for link in page.get("links", []):
                    if self._crawler._should_crawl(
                        link, seed_domain, same_domain_only,
                        exclude_patterns, include_patterns,
                    ):
                        await self._crawler._queue.add_url(link, depth=depth + 1)

        finally:
            self._crawler._active_tasks -= 1
            if (self._crawler._active_tasks == 0
                    and self._crawler._queue.pending_count() == 0
                    and self._crawler._all_done_event):
                self._crawler._all_done_event.set()

    # ------------------------------------------------------------------
    async def crawl(self) -> dict[str, dict]:
        """
        Run the full crawl using _crawl_one_with_retry workers.
        Mirrors AsyncCrawler.crawl() but dispatches our retry-aware worker.
        """
        import re
        from urllib.parse import urlparse as _up

        logger.info("AdvancedCrawler starting — %d seed URLs", len(self.start_urls))

        if self.use_sitemap and self.start_urls:
            sp = SitemapParser(self._crawler._session)
            extra = await sp.discover_sitemap_urls(self.start_urls[0])
            if extra:
                logger.info("Sitemap discovered %d additional URLs", len(extra))
                self.start_urls = list(dict.fromkeys(self.start_urls + extra))

        # Initialise queue, semaphores, rate limiter, robots parser
        self._crawler._init_crawl_components()

        seed_domain = _up(self.start_urls[0]).netloc if self.start_urls else ""
        for url in self.start_urls:
            await self._crawler._queue.add_url(url, depth=0)

        t0 = time.perf_counter()
        last_log = t0

        while True:
            if len(self._crawler.visited_urls) >= self.max_pages:
                logger.info("Reached max_pages=%d, stopping.", self.max_pages)
                break

            url = await self._crawler._queue.get_next()

            if url is None:
                if self._crawler._active_tasks == 0:
                    break
                try:
                    await asyncio.wait_for(
                        self._crawler._all_done_event.wait(), timeout=0.2)
                    break
                except asyncio.TimeoutError:
                    self._crawler._all_done_event.clear()
                    continue

            self._crawler._all_done_event.clear()
            depth = self._crawler._queue.get_depth(url)

            task = asyncio.create_task(
                self._crawl_one_with_retry(
                    url, depth, seed_domain,
                    self.same_domain_only,
                    self.exclude_patterns,
                    self.include_patterns,
                )
            )
            self._crawler._crawl_tasks.add(task)
            task.add_done_callback(self._crawler._crawl_tasks.discard)

            now = time.perf_counter()
            if now - last_log >= 5:
                stats = self._crawler._queue.get_stats()
                elapsed = now - t0
                speed = len(self._crawler.visited_urls) / elapsed if elapsed else 0
                logger.info(
                    "Progress — visited=%d | queued=%d | active=%d | %.1f p/s",
                    stats["visited"], stats["pending"],
                    self._crawler._active_tasks, speed,
                )
                last_log = now

        # Await any tasks that were still running when the loop exited
        if self._crawler._crawl_tasks:
            await asyncio.gather(*list(self._crawler._crawl_tasks), return_exceptions=True)

        self.results = self._crawler.processed_urls

        for url, page in self.results.items():
            self.stats.record_page(page)
        for url, error in self._crawler.failed_urls.items():
            self.stats.record_failure(url, error)
        self.stats.finish()
        return self.results

    def get_stats(self) -> dict:
        return self.stats.to_dict()

    def export_to_html_report(self, output_path: str) -> None:
        export_to_html_report(self.stats, output_path, self.results)

    def export_stats_to_json(self, output_path: str) -> None:
        Path(output_path).write_text(
            json.dumps(self.get_stats(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("Stats written to %s", output_path)

    async def close(self) -> None:
        await self._crawler.close()
        if self._storage:
            await self._storage.close()
        logger.info("AdvancedCrawler closed.")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()

    @classmethod
    def from_config(cls, config_path: str) -> "AdvancedCrawler":
        config = load_config(config_path)
        output_cfg = config.pop("output", {})
        storage = cls._build_storage(output_cfg)
        if storage:
            config["storage"] = storage
        return cls(**config)

    @staticmethod
    def _build_storage(output_cfg: dict):
        from storage import JSONStorage, CSVStorage, SQLiteStorage, MultiStorage
        backends = []
        if "json" in output_cfg:
            backends.append(JSONStorage(output_cfg["json"]))
        if "csv" in output_cfg:
            backends.append(CSVStorage(output_cfg["csv"]))
        if "sqlite" in output_cfg:
            backends.append(SQLiteStorage(output_cfg["sqlite"]))
        if not backends:
            return None
        return backends[0] if len(backends) == 1 else MultiStorage(backends)


# ===========================================================================
# CLI
# ===========================================================================
def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="AdvancedCrawler — async web crawler (Days 1-7)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--urls",           nargs="+", metavar="URL")
    p.add_argument("--config",         metavar="FILE")
    p.add_argument("--max-pages",      type=int,   default=50)
    p.add_argument("--max-depth",      type=int,   default=2)
    p.add_argument("--output",         metavar="FILE")
    p.add_argument("--rate-limit",     type=float, default=1.0)
    p.add_argument("--respect-robots", action="store_true",  default=True)
    p.add_argument("--no-robots",      dest="respect_robots", action="store_false")
    p.add_argument("--same-domain",    action="store_true",   default=True)
    p.add_argument("--all-domains",    dest="same_domain_only", action="store_false")
    p.add_argument("--report",         metavar="FILE")
    p.add_argument("--log-file",       metavar="FILE")
    p.add_argument("--sitemap",        action="store_true")
    return p


async def run_from_args(args: argparse.Namespace) -> None:
    if args.config:
        crawler = AdvancedCrawler.from_config(args.config)
    else:
        if not args.urls:
            print("ERROR: provide --urls or --config")
            return
        storage = None
        if args.output:
            from storage import JSONStorage
            storage = JSONStorage(args.output)
        crawler = AdvancedCrawler(
            start_urls=args.urls,
            max_pages=args.max_pages,
            max_depth=args.max_depth,
            requests_per_second=args.rate_limit,
            respect_robots=args.respect_robots,
            same_domain_only=args.same_domain_only,
            storage=storage,
            use_sitemap=args.sitemap,
            log_file=args.log_file,
        )
    async with crawler:
        await crawler.crawl()
        crawler.stats.print_summary()
        if hasattr(args, "report") and args.report:
            crawler.export_to_html_report(args.report)
            print(f"HTML report → {args.report}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = build_arg_parser()
    args = parser.parse_args()
    asyncio.run(run_from_args(args))
