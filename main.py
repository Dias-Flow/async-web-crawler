"""
async_crawler/main.py  (Day 1 demo - updated to match str API)

fetch_url()  returns str (HTML)
fetch_urls() returns dict[str, str]

This demo shows sequential vs parallel fetching and measures the speed difference.
Run with:  python main.py
"""

import asyncio
import time
from crawler import AsyncCrawler

TEST_URLS = [
    "https://example.com",
    "https://httpbin.org/html",
    "https://httpbin.org/json",
    "https://httpbin.org/uuid",
    "https://httpbin.org/user-agent",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/status/404",
    "https://httpbin.org/status/500",
    "https://this-domain-does-not-exist-xyz.com",
]


async def fetch_sequential(urls: list[str]) -> tuple[dict[str, str], float]:
    """Fetch URLs one at a time. Returns {url: html} and wall time."""
    results: dict[str, str] = {}
    async with AsyncCrawler(max_concurrent=1, max_retries=0) as crawler:
        start = time.perf_counter()
        for url in urls:
            html = await crawler.fetch_url(url)   # returns str
            results[url] = html
        elapsed = time.perf_counter() - start
    return results, elapsed


async def fetch_parallel(urls: list[str]) -> tuple[dict[str, str], float]:
    """Fetch all URLs concurrently. Returns {url: html} and wall time."""
    async with AsyncCrawler(max_concurrent=10, max_retries=1) as crawler:
        start = time.perf_counter()
        results = await crawler.fetch_urls(urls)   # returns dict[str, str]
        elapsed = time.perf_counter() - start
    return results, elapsed


def print_results_table(results: dict[str, str], title: str) -> None:
    """Print a table showing URL, content size, and success/failure."""
    col_url  = 50
    col_size = 12
    col_ok   = 6
    divider  = "-" * (col_url + col_size + col_ok + 4)

    print(f"\n{'=' * len(divider)}")
    print(f"  {title}")
    print("=" * len(divider))
    print(f"{'URL':<{col_url}} {'SIZE (KB)':>{col_size}} {'OK?':>{col_ok}}")
    print(divider)

    for url, html in results.items():
        short_url = url if len(url) <= col_url else url[:col_url-3] + "..."
        size_str  = f"{len(html)/1024:.1f}" if html else "-"
        ok_str    = "OK" if html else "FAIL"
        print(f"{short_url:<{col_url}} {size_str:>{col_size}} {ok_str:>{col_ok}}")
    print(divider)


def print_comparison(seq_time: float, par_time: float, url_count: int) -> None:
    speedup = seq_time / par_time if par_time > 0 else float("inf")
    print("\n" + "=" * 55)
    print("  PERFORMANCE COMPARISON")
    print("=" * 55)
    print(f"  URLs fetched    : {url_count}")
    print(f"  Sequential time : {seq_time:.2f}s")
    print(f"  Parallel time   : {par_time:.2f}s")
    print(f"  Speed-up        : {speedup:.1f}x faster")
    print("=" * 55 + "\n")


async def main() -> None:
    print("\n" + "=" * 55)
    print("  AsyncCrawler - Day 1 Demo")
    print("=" * 55)

    print("\n[1/2] Sequential fetch (baseline)...")
    seq_results, seq_time = await fetch_sequential(TEST_URLS)
    print_results_table(seq_results, f"Sequential ({seq_time:.2f}s total)")

    print("\n[2/2] Parallel fetch...")
    par_results, par_time = await fetch_parallel(TEST_URLS)
    print_results_table(par_results, f"Parallel ({par_time:.2f}s total)")

    print_comparison(seq_time, par_time, len(TEST_URLS))


if __name__ == "__main__":
    asyncio.run(main())
