"""
async_crawler/main_day6.py  — Day 6 demo: all three storage backends.
Run with: python main_day6.py
"""
import asyncio
import json
from pathlib import Path
from storage import JSONStorage, CSVStorage, SQLiteStorage, MultiStorage

# Fake page data matching the shape HTMLParser produces
SAMPLE_PAGES = [
    {
        "url": f"https://example.com/page{i}",
        "title": f"Page {i} — Example",
        "text": f"This is the text content of page {i}." * 10,
        "links": [f"https://example.com/page{i+1}", f"https://example.com/page{i+2}"],
        "metadata": {"description": f"Description for page {i}", "keywords": "example"},
        "text_length": 400,
        "links_count": 2,
        "status_code": 200,
        "content_type": "text/html",
    }
    for i in range(1, 11)
]

async def main():
    print("\n" + "=" * 55)
    print("  Day 6 Demo — Storage Backends")
    print("=" * 55)

    # ── MultiStorage: write to all three at once ───────────────────────
    storage = MultiStorage([
        JSONStorage("results_day6.jsonl"),
        CSVStorage("results_day6.csv"),
        SQLiteStorage("results_day6.db", batch_size=5),
    ])

    async with storage:
        for page in SAMPLE_PAGES:
            await storage.save(page)
        print(f"\nSaved {len(SAMPLE_PAGES)} pages to 3 backends.")

    # ── Verify: read back from SQLite ─────────────────────────────────
    db = SQLiteStorage("results_day6.db")
    rows = await db.query("SELECT url, title, links_count FROM pages LIMIT 5")
    await db.close()
    print("\nSQLite — first 5 rows:")
    for row in rows:
        print(f"  {row['url']:<40} {row['title']:<30} links={row['links_count']}")

    # ── Verify: count lines in JSON Lines file ─────────────────────────
    lines = Path("results_day6.jsonl").read_text().strip().splitlines()
    print(f"\nJSONL file: {len(lines)} lines (each is one JSON record)")

    # ── Verify: count rows in CSV ──────────────────────────────────────
    csv_lines = Path("results_day6.csv").read_text().strip().splitlines()
    print(f"CSV file:   {len(csv_lines) - 1} data rows + 1 header")

    print("\nStorage stats:", storage.get_stats())

asyncio.run(main())
