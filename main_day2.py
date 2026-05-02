"""
async_crawler/main_day2.py

Day 2 demo — fetch multiple pages, parse HTML, save results to JSON,
and print a statistics summary.

Run with:
    python main_day2.py
"""

import asyncio
import json
import time
from pathlib import Path

from crawler import AsyncCrawler

# ---------------------------------------------------------------------------
# Pages to crawl — a mix that exercises different HTML structures
# ---------------------------------------------------------------------------
URLS = [
    "https://example.com",
    "https://httpbin.org/html",
    "https://httpbin.org/links/10/0",   # page full of <a> links
    "https://en.wikipedia.org/wiki/Python_(programming_language)",
]

OUTPUT_FILE = Path("results_day2.json")


# ---------------------------------------------------------------------------
# Helper — compact summary for console output
# ---------------------------------------------------------------------------
def print_summary(pages: dict[str, dict]) -> None:
    """Print a one-line summary per page."""
    divider = "-" * 70
    print(f"\n{'=' * 70}")
    print("  Day 2 — Parse Results Summary")
    print("=" * 70)
    print(f"{'URL':<42} {'TITLE':<20} {'LINKS':>6} {'TEXT':>7}")
    print(divider)

    for url, data in pages.items():
        short_url = url if len(url) <= 42 else url[:39] + "…"
        title = (data.get("title") or "—")[:20]
        links = data.get("links_count", 0)
        text_len = data.get("text_length", 0)
        error = data.get("error")
        if error:
            print(f"{short_url:<42} ERROR: {error}")
        else:
            print(f"{short_url:<42} {title:<20} {links:>6} {text_len:>7}")

    print(divider)
    ok = sum(1 for d in pages.values() if not d.get("error"))
    print(f"  Total: {ok}/{len(pages)} pages parsed successfully")
    print("=" * 70 + "\n")


def print_detail(data: dict) -> None:
    """Print detailed parse results for one page."""
    print(f"\n--- Detail: {data['url']} ---")
    print(f"  Title       : {data.get('title')}")
    print(f"  Description : {(data.get('metadata') or {}).get('description', '—')}")
    print(f"  Text length : {data.get('text_length', 0):,} chars")
    print(f"  Links       : {data.get('links_count', 0)}")
    print(f"  Images      : {data.get('images_count', 0)}")
    print(f"  H1 headings : {data.get('headings', {}).get('h1', [])}")
    print(f"  Fetch time  : {data.get('fetch_elapsed', 0):.2f}s")
    if data.get("links"):
        print(f"  First 3 links:")
        for link in data["links"][:3]:
            print(f"    • {link}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main() -> None:
    print("\n" + "=" * 70)
    print("  AsyncCrawler — Day 2 Demo: HTML Parsing")
    print("=" * 70)

    t0 = time.perf_counter()

    async with AsyncCrawler(max_concurrent=5, max_retries=1) as crawler:
        # fetch_and_parse_urls runs all URLs in parallel, each fetched then parsed
        pages = await crawler.fetch_and_parse_urls(URLS)

    wall = time.perf_counter() - t0

    # --- Console summary ---
    print_summary(pages)

    # --- Detailed output for first two successful pages ---
    for data in list(pages.values())[:2]:
        print_detail(data)

    # --- Save full results to JSON ---
    # Convert to a JSON-friendly list; drop raw text to keep the file small
    output = []
    for data in pages.values():
        slim = {k: v for k, v in data.items() if k != "text"}
        output.append(slim)

    OUTPUT_FILE.write_text(json.dumps(output, indent=2, ensure_ascii=False))
    print(f"\nFull results saved to → {OUTPUT_FILE.resolve()}")
    print(f"Total wall time: {wall:.2f}s\n")


if __name__ == "__main__":
    asyncio.run(main())
