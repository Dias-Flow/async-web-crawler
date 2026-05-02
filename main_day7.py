"""
async_crawler/main_day7.py  — Day 7 demo: AdvancedCrawler full integration.
Run with: python main_day7.py
"""
import asyncio
from storage import MultiStorage, JSONStorage, CSVStorage, SQLiteStorage
from advanced_crawler import AdvancedCrawler

async def main():
    print("\n" + "=" * 55)
    print("  Day 7 Demo — AdvancedCrawler Full Integration")
    print("=" * 55)

    storage = MultiStorage([
        JSONStorage("results_day7.jsonl"),
        CSVStorage("results_day7.csv"),
        SQLiteStorage("results_day7.db"),
    ])

    async with AdvancedCrawler(
        start_urls=["https://example.com"],
        max_pages=5,
        max_depth=1,
        requests_per_second=0.5,
        min_delay=1.0,
        respect_robots=True,
        same_domain_only=True,
        storage=storage,
        use_sitemap=False,
    ) as crawler:
        await crawler.crawl()
        crawler.stats.print_summary()
        crawler.export_to_html_report("report_day7.html")
        crawler.export_stats_to_json("stats_day7.json")

    print("\nFiles written:")
    print("  results_day7.jsonl")
    print("  results_day7.csv")
    print("  results_day7.db")
    print("  report_day7.html   ← open in browser!")
    print("  stats_day7.json")

asyncio.run(main())
