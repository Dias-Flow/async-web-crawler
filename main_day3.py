"""
async_crawler/main_day3.py

Day 3 demo — crawl example.com with depth/page limits.
Shows real-time progress, queue stats, and saves results to JSON.

Run with:
    python main_day3.py
"""

import asyncio
import json
from pathlib import Path
from crawler import AsyncCrawler


async def main() -> None:
    print("\n" + "=" * 60)
    print("  Day 3 Demo — Site Crawl with Queue + Depth Control")
    print("=" * 60 + "\n")

    async with AsyncCrawler(
        max_concurrent=3,   # keep it gentle for demo
        max_depth=1,        # seed page + its direct links
        requests_per_second=0.5,
        min_delay=1.0,
        respect_robots=True,
    ) as crawler:

        results = await crawler.crawl(
            start_urls=["https://example.com"],
            max_pages=10,
            same_domain_only=True,
        )

    # ── Summary ────────────────────────────────────────────────────────
    print(f"\nPages crawled   : {len(results)}")
    print(f"Failed URLs     : {len(crawler.failed_urls)}")

    for url, page in list(results.items())[:5]:
        print(f"\n  {url}")
        print(f"    title  : {page.get('title')}")
        print(f"    links  : {page.get('links_count', 0)}")
        print(f"    text   : {page.get('text_length', 0):,} chars")

    # ── Save ───────────────────────────────────────────────────────────
    out = [
        {k: v for k, v in page.items() if k != "text"}
        for page in results.values()
    ]
    Path("results_day3.json").write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print("\nSaved → results_day3.json")


if __name__ == "__main__":
    asyncio.run(main())
