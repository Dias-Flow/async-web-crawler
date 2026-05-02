"""
async_crawler/main.py

Entry-point script that demonstrates the AsyncCrawler.

What this script does:
  1. Runs a SEQUENTIAL fetch (one URL at a time) and records how long it takes.
  2. Runs a PARALLEL fetch (all URLs at once, limited by semaphore) and records the time.
  3. Prints a side-by-side comparison table so the speed advantage is obvious.
  4. Shows per-URL results: status, size, time, success/failure.

Run with:
    python main.py
"""

import asyncio
import time

from crawler import AsyncCrawler, FetchResult

# ---------------------------------------------------------------------------
# Test URLs — a mix of real pages, intentional errors, and artificial delays
# ---------------------------------------------------------------------------
TEST_URLS = [
    # Fast pages
    "https://example.com",
    "https://httpbin.org/html",
    "https://httpbin.org/json",
    "https://httpbin.org/uuid",
    "https://httpbin.org/user-agent",
    # Artificial delays (simulates slow servers)
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    # Error cases — should be handled gracefully without crashing
    "https://httpbin.org/status/404",   # Not Found
    "https://httpbin.org/status/500",   # Internal Server Error
    "https://this-domain-does-not-exist-xyz.com",  # DNS failure
]


# ---------------------------------------------------------------------------
# Sequential fetch helper — fetches URLs one after another (no parallelism)
# ---------------------------------------------------------------------------
async def fetch_sequential(urls: list[str]) -> tuple[dict[str, FetchResult], float]:
    """
    Fetches each URL one at a time in order.
    Returns results dict and total wall-clock time.
    This establishes the baseline we compare parallel speed against.
    """
    results: dict[str, FetchResult] = {}

    # We still use AsyncCrawler so the comparison is fair (same session, same logic)
    async with AsyncCrawler(max_concurrent=1, max_retries=0) as crawler:
        start = time.perf_counter()
        for url in urls:
            result = await crawler.fetch_url(url)
            results[url] = result
        elapsed = time.perf_counter() - start

    return results, elapsed


# ---------------------------------------------------------------------------
# Parallel fetch — all URLs are submitted simultaneously
# ---------------------------------------------------------------------------
async def fetch_parallel(urls: list[str]) -> tuple[dict[str, FetchResult], float]:
    """
    Fetches all URLs concurrently.
    Returns results dict and total wall-clock time.
    """
    async with AsyncCrawler(max_concurrent=10, max_retries=1) as crawler:
        start = time.perf_counter()
        results = await crawler.fetch_urls(urls)
        elapsed = time.perf_counter() - start

    return results, elapsed


# ---------------------------------------------------------------------------
# Pretty-print helpers
# ---------------------------------------------------------------------------
def print_results_table(results: dict[str, FetchResult], title: str) -> None:
    """Renders a simple ASCII table summarising each URL's outcome."""
    col_url = 50
    col_status = 8
    col_size = 10
    col_time = 8
    col_ok = 6

    divider = "-" * (col_url + col_status + col_size + col_time + col_ok + 8)
    header = (
        f"{'URL':<{col_url}} {'STATUS':>{col_status}} {'SIZE (KB)':>{col_size}} "
        f"{'TIME (s)':>{col_time}} {'OK?':>{col_ok}}"
    )

    print(f"\n{'=' * len(divider)}")
    print(f"  {title}")
    print("=" * len(divider))
    print(header)
    print(divider)

    for url, r in results.items():
        short_url = url if len(url) <= col_url else url[: col_url - 3] + "…"
        status_str = str(r.status) if r.status else "—"
        size_str = f"{len(r.content) / 1024:.1f}" if r.content else "—"
        ok_str = "✓" if r.success else "✗"
        print(
            f"{short_url:<{col_url}} {status_str:>{col_status}} {size_str:>{col_size}} "
            f"{r.elapsed:>{col_time}.2f} {ok_str:>{col_ok}}"
        )

    print(divider)


def print_comparison(seq_time: float, par_time: float, url_count: int) -> None:
    """Prints the speed comparison summary."""
    speedup = seq_time / par_time if par_time > 0 else float("inf")
    print("\n" + "=" * 55)
    print("  PERFORMANCE COMPARISON")
    print("=" * 55)
    print(f"  URLs fetched          : {url_count}")
    print(f"  Sequential time       : {seq_time:.2f}s")
    print(f"  Parallel time         : {par_time:.2f}s")
    print(f"  Speed-up              : {speedup:.1f}x faster")
    print("=" * 55 + "\n")


# ---------------------------------------------------------------------------
# Main coroutine
# ---------------------------------------------------------------------------
async def main() -> None:
    print("\n" + "=" * 55)
    print("  AsyncCrawler — Day 1 Demo")
    print("=" * 55)

    # --- Sequential run ---
    print("\n[1/2] Running SEQUENTIAL fetch (baseline)…")
    seq_results, seq_time = await fetch_sequential(TEST_URLS)
    print_results_table(seq_results, f"Sequential results  ({seq_time:.2f}s total)")

    # --- Parallel run ---
    print("\n[2/2] Running PARALLEL fetch…")
    par_results, par_time = await fetch_parallel(TEST_URLS)
    print_results_table(par_results, f"Parallel results  ({par_time:.2f}s total)")

    # --- Comparison ---
    print_comparison(seq_time, par_time, len(TEST_URLS))


if __name__ == "__main__":
    asyncio.run(main())
