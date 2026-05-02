"""
async_crawler/main_books.py

Demo script for the books.toscrape.com scraper.

Run with:
    python main_books.py

What this script does:
  1. Scrapes 50 books from books.toscrape.com (change max_books for more)
  2. Saves results to books.json, books.csv, and books.db
  3. Prints a sample table to the console
  4. Prints statistics (genres, ratings, etc.)

Expected runtime for 50 books: ~30-60 seconds (rate-limited to be polite).
Expected runtime for all 1000 books: ~10-15 minutes.
"""

import asyncio
import json
import logging
from pathlib import Path

from books_scraper import BooksScraper
from storage import MultiStorage, JSONStorage, CSVStorage, SQLiteStorage

# Show INFO logs so you can see the scraper working
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)


async def main() -> None:
    print("\n" + "=" * 60)
    print("  books.toscrape.com Scraper Demo")
    print("=" * 60)
    print("  Scraping 50 books (change max_books for more)...")
    print("  Saving to: books.json, books.csv, books.db\n")

    # Configure storage: write to all three formats simultaneously
    storage = MultiStorage([
        JSONStorage("books.json"),
        CSVStorage("books.csv"),
        SQLiteStorage("books.db", batch_size=10),
    ])

    async with BooksScraper(
        max_concurrent=5,       # 5 parallel requests at a time
        max_books=50,           # scrape only 50 books for the demo
                                # change to 1000 for the full dataset
        requests_per_second=2.0,
        storage=storage,
    ) as scraper:
        books = await scraper.scrape()

    # ── Console output ─────────────────────────────────────────────────
    scraper.print_sample(n=10)

    stats = scraper.get_stats()
    print("\n  Scrape statistics:")
    print(f"    Books scraped       : {stats['total_scraped']}")
    print(f"    Failed              : {stats['total_failed']}")
    print(f"    Unique genres found : {stats['genres_found']}")
    print(f"    Top 5 genres        : {stats['top_genres']}")
    print(f"    Rating distribution : {stats['rating_distribution']}")

    # ── Verify JSON output ─────────────────────────────────────────────
    if Path("books.json").exists():
        lines = Path("books.json").read_text().strip().splitlines()
        print(f"\n  books.json  : {len(lines)} lines (one book per line)")

    # ── Verify CSV output ──────────────────────────────────────────────
    if Path("books.csv").exists():
        csv_lines = Path("books.csv").read_text().strip().splitlines()
        print(f"  books.csv   : {len(csv_lines) - 1} data rows + 1 header")

    # ── Verify SQLite output ───────────────────────────────────────────
    if Path("books.db").exists():
        from storage import SQLiteStorage as SQL
        db = SQL("books.db")
        rows = await db.query("SELECT COUNT(*) as n FROM pages")
        await db.close()
        print(f"  books.db    : {rows[0]['n']} rows in 'pages' table")

    print("\n  Done! Open books.csv in Excel or books.db in DB Browser for SQLite.")


if __name__ == "__main__":
    asyncio.run(main())
