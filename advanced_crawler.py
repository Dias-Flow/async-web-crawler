"""
async_crawler/advanced_crawler.py  (Day 7)

The final integration layer. AdvancedCrawler wires together all
components from Days 1–6 and adds Day-7 features:
  CrawlerStats   — collects and reports statistics about the crawl
  SitemapParser  — discovers URLs from sitemap.xml files
  HTML report    — generates a standalone report.html you can open in a browser
  Config loader  — reads YAML or JSON config files
  CLI            — run the crawler from the command line without writing Python

HOW THE PIECES FIT TOGETHER:
  AdvancedCrawler
    │
    ├── AsyncCrawler (Days 1-4)    ← does the actual fetching and crawling
    │     ├── HTMLParser (Day 2)
    │     ├── CrawlerQueue (Day 3)
    │     ├── SemaphoreManager (Day 3)
    │     ├── RateLimiter (Day 4)
    │     └── RobotsParser (Day 4)
    │
    ├── RetryStrategy (Day 5)      ← wraps fetches with smart retry logic
    ├── DataStorage (Day 6)        ← saves results to JSON/CSV/SQLite
    └── CrawlerStats (Day 7)       ← counts pages, speeds, errors
"""

import argparse
import asyncio
import json
import logging
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urljoin

import aiohttp

logger = logging.getLogger("AdvancedCrawler")


# ===========================================================================
# CrawlerStats — statistics collector
# ===========================================================================
class CrawlerStats:
    """
    Accumulates statistics as the crawl runs.

    WHY a separate class and not just counters in AsyncCrawler?
      Statistics are a reporting concern, separate from crawling logic.
      By isolating them here:
        - AsyncCrawler stays focused on HTTP
        - CrawlerStats can be tested independently with fake data
        - You can add new stats without touching AsyncCrawler

    USAGE:
        stats = CrawlerStats()
        stats.record_page(page_data)      # call for each successful page
        stats.record_failure(url, error)  # call for each failed page
        stats.finish()                    # call when crawl ends
        print(stats.to_dict())            # get all numbers as a dict
        stats.print_summary()             # print a formatted table
    """

    def __init__(self) -> None:
        self._start_time: float = time.perf_counter()
        self._end_time: Optional[float] = None

        self.total_pages: int = 0
        self.successful: int = 0
        self.failed: int = 0

        # Counter acts like a dict but starts every new key at 0 automatically
        # status_codes example: {200: 45, 404: 3, 503: 1}
        self.status_codes: Counter = Counter()

        # domain_counts example: {"example.com": 12, "blog.example.com": 5}
        self.domain_counts: Counter = Counter()

        self._total_fetch_time: float = 0.0
        self.error_types: Counter = Counter()

    def record_page(self, page_data: dict) -> None:
        """
        Record one successfully fetched+parsed page.
        Called by AdvancedCrawler.crawl() after each page completes.
        """
        self.total_pages += 1
        self.successful += 1
        self._total_fetch_time += page_data.get("fetch_elapsed", 0.0)

        # Extract "example.com" from the full URL
        domain = urlparse(page_data.get("url", "")).netloc
        if domain:
            self.domain_counts[domain] += 1

        sc = page_data.get("status_code")
        if sc:
            self.status_codes[sc] += 1

    def record_failure(self, url: str, error: str) -> None:
        """Record one permanently failed URL."""
        self.total_pages += 1
        self.failed += 1
        # Use the first word of the error string as a rough category
        error_type = error.split(":")[0].strip() if error else "Unknown"
        self.error_types[error_type] += 1

    def finish(self) -> None:
        """Call this when the crawl ends to freeze the elapsed time."""
        self._end_time = time.perf_counter()

    @property
    def elapsed_seconds(self) -> float:
        end = self._end_time or time.perf_counter()
        return round(end - self._start_time, 2)

    @property
    def pages_per_second(self) -> float:
        """Average crawl speed. 0 if no pages were crawled."""
        if self.elapsed_seconds == 0:
            return 0.0
        return round(self.successful / self.elapsed_seconds, 2)

    @property
    def avg_fetch_time(self) -> float:
        """Average time spent fetching + parsing one page."""
        if self.successful == 0:
            return 0.0
        return round(self._total_fetch_time / self.successful, 3)

    def to_dict(self) -> dict:
        """
        All stats as a plain dict (JSON-serialisable).
        Used by export_stats_to_json() and the HTML report generator.
        """
        return {
            "total_pages":      self.total_pages,
            "successful":       self.successful,
            "failed":           self.failed,
            "elapsed_seconds":  self.elapsed_seconds,
            "pages_per_second": self.pages_per_second,
            "avg_fetch_time":   self.avg_fetch_time,
            "status_codes":     dict(self.status_codes),
            # most_common(10) returns the 10 domains with most pages
            "top_domains":      dict(self.domain_counts.most_common(10)),
            "error_types":      dict(self.error_types),
        }

    def print_summary(self) -> None:
        """Print a formatted statistics table to the console."""
        s = self.to_dict()
        print("\n" + "=" * 55)
        print("  CRAWL STATISTICS")
        print("=" * 55)
        print(f"  Total pages processed : {s['total_pages']}")
        print(f"  Successful            : {s['successful']}")
        print(f"  Failed                : {s['failed']}")
        print(f"  Elapsed time          : {s['elapsed_seconds']}s")
        print(f"  Speed                 : {s['pages_per_second']} pages/s")
        print(f"  Avg fetch time        : {s['avg_fetch_time']}s")
        if s["status_codes"]:
            print(f"  Status codes          : {s['status_codes']}")
        if s["top_domains"]:
            print(f"  Top domains           : {s['top_domains']}")
        if s["error_types"]:
            print(f"  Error types           : {s['error_types']}")
        print("=" * 55)


# ===========================================================================
# SitemapParser — URL discovery via sitemap.xml
# ===========================================================================
class SitemapParser:
    """
    Fetches and parses XML sitemaps to discover URLs.

    WHY sitemaps?
      Link-following crawlers miss pages that are not linked from anywhere.
      Sitemaps are the site owner's authoritative list of ALL their URLs.
      Using a sitemap as seed gives better coverage with fewer hops.

    TWO SITEMAP TYPES:
      1. Regular sitemap (<urlset>):
         Contains <url><loc>https://example.com/page1</loc></url> entries.
         Just a list of page URLs.

      2. Sitemap index (<sitemapindex>):
         Contains <sitemap><loc>https://example.com/sitemap1.xml</loc></sitemap>
         entries pointing to OTHER sitemaps. Think of it as a "table of contents"
         for large sites that have thousands of pages split across multiple sitemaps.
         We handle this recursively: fetch the index → fetch each child sitemap.

    DEDUPLICATION:
      _visited_sitemaps tracks which sitemap URLs we've already fetched.
      Prevents infinite loops if sitemaps reference each other.
    """

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session
        self._visited_sitemaps: set[str] = set()

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        """
        Fetch one sitemap URL and return all page URLs found inside it.
        Automatically recurses into child sitemaps if it's a sitemap index.
        """
        if sitemap_url in self._visited_sitemaps:
            return []  # already processed this sitemap — avoid loops
        self._visited_sitemaps.add(sitemap_url)

        try:
            async with self._session.get(sitemap_url) as resp:
                if resp.status != 200:
                    logger.warning("Sitemap %s returned HTTP %d", sitemap_url, resp.status)
                    return []
                text = await resp.text()
        except Exception as exc:
            logger.warning("Could not fetch sitemap %s: %s", sitemap_url, exc)
            return []

        # Parse the XML using Python's built-in xml.etree module
        # WHY not BeautifulSoup? Sitemaps are well-formed XML, not HTML.
        # ElementTree is faster and more correct for XML.
        import xml.etree.ElementTree as ET
        try:
            root = ET.fromstring(text)
        except ET.ParseError as exc:
            logger.warning("Sitemap XML parse error at %s: %s", sitemap_url, exc)
            return []

        def strip_ns(tag: str) -> str:
            """
            Remove XML namespace from tag names.
            Sitemaps use: {http://www.sitemaps.org/schemas/sitemap/0.9}urlset
            We want just: urlset
            'tag.split("}")[-1]' takes the part after the last '}'
            """
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
                            # This is a sitemap index — recursively fetch child sitemaps
                            child_urls = await self.fetch_sitemap(loc)
                            urls.extend(child_urls)
                        else:
                            # This is a regular sitemap — loc is a page URL
                            urls.append(loc)

        logger.info("Sitemap %s yielded %d URLs", sitemap_url, len(urls))
        return urls

    async def discover_sitemap_urls(self, base_url: str) -> list[str]:
        """
        Try common sitemap locations for a site and return all discovered URLs.
        Most sites put their sitemap at /sitemap.xml or /sitemap_index.xml.
        """
        parsed = urlparse(base_url)
        origin = f"{parsed.scheme}://{parsed.netloc}"
        candidates = [
            urljoin(origin, "/sitemap.xml"),
            urljoin(origin, "/sitemap_index.xml"),
        ]
        all_urls: list[str] = []
        for candidate in candidates:
            urls = await self.fetch_sitemap(candidate)
            all_urls.extend(urls)

        # dict.fromkeys preserves order while deduplicating (unlike set())
        return list(dict.fromkeys(all_urls))


# ===========================================================================
# Config loader
# ===========================================================================
def load_config(path: str) -> dict:
    """
    Load a YAML or JSON config file and return it as a Python dict.

    WHY YAML?
      YAML is more readable than JSON for config files:
        JSON: {"max_pages": 50, "start_urls": ["https://example.com"]}
        YAML: max_pages: 50\nstart_urls:\n  - https://example.com

      YAML also supports comments (# this is a comment), JSON doesn't.

    WHY JSON fallback?
      PyYAML is an optional dependency. If the user hasn't installed it,
      they can still use JSON config files.

    The returned dict is passed directly to AdvancedCrawler(**config),
    so the keys must match AdvancedCrawler's __init__ parameter names.
    """
    config_path = Path(path)
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    text = config_path.read_text(encoding="utf-8")

    if config_path.suffix in (".yaml", ".yml"):
        try:
            import yaml
            return yaml.safe_load(text) or {}
        except ImportError:
            logger.warning("PyYAML not installed, trying JSON fallback for %s", path)

    return json.loads(text)


# ===========================================================================
# HTML report generator
# ===========================================================================
def export_to_html_report(stats: CrawlerStats, output_path: str, pages: dict) -> None:
    """
    Write a self-contained HTML report file.

    WHY self-contained?
      All CSS is inline in <style> tags.
      No external files (Bootstrap, Chart.js CDN) needed.
      You can open the file offline and it looks the same everywhere.

    WHAT'S IN THE REPORT:
      - Summary stat boxes (total pages, speed, etc.)
      - Status code distribution table
      - Top domains by page count
      - Full table of crawled pages with URL, title, link count, text length
    """
    s = stats.to_dict()

    # Build one <tr> per crawled page (capped at 500 to keep file small)
    rows = ""
    for url, page in list(pages.items())[:500]:
        title    = (page.get("title") or "—")[:60]
        links    = page.get("links_count", 0)
        text_len = page.get("text_length", 0)
        rows += (
            f"<tr>"
            f"<td><a href='{url}' target='_blank'>{url[:70]}</a></td>"
            f"<td>{title}</td>"
            f"<td>{links}</td>"
            f"<td>{text_len:,}</td>"
            f"</tr>\n"
        )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Crawl Report</title>
<style>
  body  {{ font-family: Arial, sans-serif; margin: 2rem; color: #222; }}
  h1    {{ color: #2c5f8a; }}
  h2    {{ color: #2c5f8a; margin-top: 2rem; }}
  table {{ border-collapse: collapse; width: 100%; margin-bottom: 2rem; }}
  th    {{ background: #2c5f8a; color: white; padding: 8px 12px; text-align: left; }}
  td    {{ padding: 6px 12px; border-bottom: 1px solid #ddd; }}
  tr:hover td {{ background: #f0f4f8; }}
  a     {{ color: #2c5f8a; }}
  /* Stat boxes — the big number cards at the top */
  .stat {{ display: inline-block; background: #e8f0fe; border-radius: 8px;
           padding: 1rem 2rem; margin: 0.5rem; text-align: center; }}
  .stat .num {{ font-size: 2rem; font-weight: bold; color: #2c5f8a; }}
  .stat .lbl {{ font-size: 0.85rem; color: #555; }}
</style>
</head>
<body>
<h1>🕷 Async Crawler Report</h1>
<p>Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}</p>

<!-- Big stat boxes at the top -->
<div>
  <div class="stat">
    <div class="num">{s['total_pages']}</div>
    <div class="lbl">Total pages</div>
  </div>
  <div class="stat">
    <div class="num">{s['successful']}</div>
    <div class="lbl">Successful</div>
  </div>
  <div class="stat">
    <div class="num">{s['failed']}</div>
    <div class="lbl">Failed</div>
  </div>
  <div class="stat">
    <div class="num">{s['elapsed_seconds']}s</div>
    <div class="lbl">Elapsed</div>
  </div>
  <div class="stat">
    <div class="num">{s['pages_per_second']}</div>
    <div class="lbl">Pages/sec</div>
  </div>
</div>

<h2>Status Codes</h2>
<table>
  <tr><th>Status</th><th>Count</th></tr>
  {''.join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in s['status_codes'].items())}
</table>

<h2>Top Domains</h2>
<table>
  <tr><th>Domain</th><th>Pages crawled</th></tr>
  {''.join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in s['top_domains'].items())}
</table>

<h2>Crawled Pages ({len(pages)} total, showing up to 500)</h2>
<table>
  <tr><th>URL</th><th>Title</th><th>Links</th><th>Text length</th></tr>
  {rows}
</table>
</body>
</html>"""

    Path(output_path).write_text(html, encoding="utf-8")
    logger.info("HTML report written to %s", output_path)


# ===========================================================================
# AdvancedCrawler — the final integration class
# ===========================================================================
class AdvancedCrawler:
    """
    The "director" class. Coordinates all components.

    COMPARED TO AsyncCrawler (Days 1-4):
      + Uses RetryStrategy for smarter retry logic
      + Uses DataStorage to persist results while crawling
      + Uses CrawlerStats to collect metrics
      + Supports sitemap.xml URL discovery
      + Loadable from a YAML/JSON config file
      + Has an HTML report exporter
      + Has a CLI interface (see __main__ block at the bottom)
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
        storage=None,              # Optional[DataStorage]
        use_sitemap: bool = False,
        user_agent: str = "AdvancedCrawler/1.0 (educational; aiohttp)",
        connect_timeout: float = 10.0,
        read_timeout: float = 30.0,
        max_retries: int = 3,
        log_file: Optional[str] = None,
    ) -> None:
        self.start_urls    = start_urls or []
        self.max_pages     = max_pages
        self.same_domain_only = same_domain_only
        self.exclude_patterns = exclude_patterns or []
        self.include_patterns = include_patterns or []
        self.use_sitemap   = use_sitemap
        self._storage      = storage

        # Optional: also write logs to a file
        if log_file:
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
            ))
            logging.getLogger().addHandler(fh)

        # Import AsyncCrawler and RetryStrategy here (not at top of file) to
        # avoid circular imports and keep the import chain clear.
        from crawler import AsyncCrawler
        from retry_strategy import RetryStrategy, TransientError, NetworkError

        # AsyncCrawler handles all HTTP, parsing, queueing, rate limiting
        self._crawler = AsyncCrawler(
            max_concurrent=max_concurrent,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            max_retries=0,         # RetryStrategy handles retries now, not AsyncCrawler
            max_depth=max_depth,
            requests_per_second=requests_per_second,
            min_delay=min_delay,
            jitter=jitter,
            respect_robots=respect_robots,
            user_agent=user_agent,
        )

        # RetryStrategy wraps individual fetch calls with exponential back-off
        self._retry = RetryStrategy(
            max_retries=max_retries,
            backoff_factor=2.0,
            retry_on=[TransientError, NetworkError],
        )

        self.stats = CrawlerStats()
        self.results: dict[str, dict] = {}

    @classmethod
    def from_config(cls, config_path: str) -> "AdvancedCrawler":
        """
        Create an AdvancedCrawler from a YAML or JSON config file.

        The 'output' section of the config is handled specially:
        it gets converted to a DataStorage object before being passed
        to __init__.

        Example config.yaml:
            start_urls: [https://example.com]
            max_pages: 50
            output:
              json: results.jsonl
              sqlite: results.db
        """
        config = load_config(config_path)

        # Pop 'output' from config before passing to __init__
        # (because __init__ doesn't have an 'output' parameter)
        output_cfg = config.pop("output", {})
        storage = cls._build_storage(output_cfg)
        if storage:
            config["storage"] = storage

        return cls(**config)

    @staticmethod
    def _build_storage(output_cfg: dict):
        """
        Convert the 'output' config section to a DataStorage instance.
        If multiple formats are specified, returns a MultiStorage that
        writes to all of them simultaneously.
        """
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
        # Single backend → return it directly (no overhead from MultiStorage)
        return backends[0] if len(backends) == 1 else MultiStorage(backends)

    async def crawl(self) -> dict[str, dict]:
        """
        Run the full crawl and return {url: page_data} for all successful pages.

        SEQUENCE:
          1. If use_sitemap=True, fetch sitemap.xml to expand the seed URL list
          2. Hand off to AsyncCrawler.crawl() for the actual work
          3. Save each result to storage (if storage was configured)
          4. Record statistics for each page
          5. Return the results dict
        """
        logger.info("AdvancedCrawler starting — %d seed URLs", len(self.start_urls))

        # Optional: discover more start URLs via sitemap.xml
        if self.use_sitemap and self.start_urls:
            sitemap_parser = SitemapParser(self._crawler._session)
            extra = await sitemap_parser.discover_sitemap_urls(self.start_urls[0])
            if extra:
                logger.info("Sitemap discovered %d additional URLs", len(extra))
                # Merge with existing start_urls, keeping order, removing duplicates
                self.start_urls = list(dict.fromkeys(self.start_urls + extra))

        # Main crawl — AsyncCrawler handles queue, depth, rate limiting, robots.txt
        results = await self._crawler.crawl(
            start_urls=self.start_urls,
            max_pages=self.max_pages,
            same_domain_only=self.same_domain_only,
            exclude_patterns=self.exclude_patterns,
            include_patterns=self.include_patterns,
        )
        self.results = results

        # Save results and record stats
        for url, page in results.items():
            self.stats.record_page(page)
            if self._storage:
                try:
                    await self._storage.save(page)
                except Exception as exc:
                    logger.error("Storage save failed for %s: %s", url, exc)

        for url, error in self._crawler.failed_urls.items():
            self.stats.record_failure(url, error)

        self.stats.finish()
        return results

    def get_stats(self) -> dict:
        """Return statistics as a plain dict."""
        return self.stats.to_dict()

    def export_to_html_report(self, output_path: str) -> None:
        """Generate a self-contained HTML report and save it to output_path."""
        export_to_html_report(self.stats, output_path, self.results)

    def export_stats_to_json(self, output_path: str) -> None:
        """Write the statistics dict to a JSON file."""
        Path(output_path).write_text(
            json.dumps(self.get_stats(), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info("Stats written to %s", output_path)

    async def close(self) -> None:
        """Shutdown: close HTTP session and flush/close storage."""
        await self._crawler.close()
        if self._storage:
            await self._storage.close()
        logger.info("AdvancedCrawler closed.")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()


# ===========================================================================
# CLI interface — run crawler from terminal without writing Python
# ===========================================================================
def build_arg_parser() -> argparse.ArgumentParser:
    """
    Build the command-line argument parser.

    argparse is Python's standard library for CLI argument parsing.
    It automatically generates --help text and validates argument types.

    Examples:
        python advanced_crawler.py --urls https://example.com --max-pages 30
        python advanced_crawler.py --config config.yaml --report report.html
        python advanced_crawler.py --urls https://example.com --no-robots
    """
    p = argparse.ArgumentParser(
        description="AdvancedCrawler — async web crawler (Days 1-7)",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--urls",           nargs="+", metavar="URL",  help="Seed URLs")
    p.add_argument("--config",         metavar="FILE",            help="YAML/JSON config file")
    p.add_argument("--max-pages",      type=int,   default=50,    help="Max pages to crawl")
    p.add_argument("--max-depth",      type=int,   default=2,     help="Max link depth")
    p.add_argument("--output",         metavar="FILE",            help="Save results to JSON file")
    p.add_argument("--rate-limit",     type=float, default=1.0,   help="Requests per second")
    p.add_argument("--respect-robots", action="store_true", default=True)
    p.add_argument("--no-robots",      dest="respect_robots",  action="store_false")
    p.add_argument("--same-domain",    action="store_true",    default=True)
    p.add_argument("--all-domains",    dest="same_domain_only",action="store_false")
    p.add_argument("--report",         metavar="FILE",            help="HTML report path")
    p.add_argument("--log-file",       metavar="FILE",            help="Also write logs to file")
    p.add_argument("--sitemap",        action="store_true",       help="Use sitemap.xml")
    return p


async def run_from_args(args: argparse.Namespace) -> None:
    """Execute a crawl from parsed CLI arguments."""
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


# This block runs only when you execute: python advanced_crawler.py
# It does NOT run when another file does: from advanced_crawler import AdvancedCrawler
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    parser = build_arg_parser()
    args = parser.parse_args()
    asyncio.run(run_from_args(args))
