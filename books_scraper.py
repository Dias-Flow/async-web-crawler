"""
async_crawler/books_scraper.py

Homework Part 2 — scraper for http://books.toscrape.com/

WHY THIS FILE EXISTS:
  books.toscrape.com is a fake bookshop made specifically for practising scraping.
  It is safe to scrape (no robots.txt restrictions, no login, no JS rendering).

  This module is a CONCRETE APPLICATION of the generic tools we built in Days 1-6:
    - AsyncCrawler (Day 1-4)  fetches pages in parallel
    - HTMLParser (Day 2)      finds the data on each page
    - DataStorage (Day 6)     saves books to JSON, CSV, and SQLite

HOW THE SITE IS STRUCTURED:
  http://books.toscrape.com/
    └── catalogue/page-1.html      ← listing page (20 books per page)
    └── catalogue/page-2.html
    └── ...
    └── catalogue/page-50.html     ← 1000 books total, 50 pages

  Each listing page has 20 book cards like:
    <article class="product_pod">
      <a href="a-light-in-the-attic_1000/index.html">...</a>
      <p class="price_color">£51.77</p>
      <p class="star-rating Three">...</p>
    </article>

  Each book card links to a DETAIL page, e.g.:
    http://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html

  The detail page has a table with all the fields we need:
    <table class="table table-striped">
      <tr><th>UPC</th>               <td>a897fe39b1053632</td></tr>
      <tr><th>Product Type</th>      <td>Books</td></tr>
      <tr><th>Price (excl. tax)</th> <td>£51.77</td></tr>
      <tr><th>Price (incl. tax)</th> <td>£51.77</td></tr>
      <tr><th>Tax</th>               <td>£0.00</td></tr>
      <tr><th>Availability</th>      <td>In stock (22 available)</td></tr>
      <tr><th>Number of reviews</th> <td>0</td></tr>
    </table>

DATA FLOW:
  1. BooksListingParser   discovers book-detail URLs from each catalogue page
  2. BooksDetailParser    extracts the 7 required fields from each detail page
  3. BooksScraper         orchestrates fetching + parsing + saving
"""

import asyncio
import logging
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

logger = logging.getLogger("BooksScraper")

# The base URL of the practice site
BASE_URL = "http://books.toscrape.com/"


# ===========================================================================
# Book — data container for one book's fields
# ===========================================================================
@dataclass
class Book:
    """
    All the data we extract for one book.

    WHY a dataclass?
      - Gives us a clean, named container (not a messy plain dict)
      - Autocomplete in PyCharm: book.upc, book.price_excl_tax, etc.
      - asdict(book) converts it to a plain dict for storage/JSON export
      - Easy to print: print(book) shows all fields neatly

    All fields are Optional[str] because any of them MIGHT be missing
    if the page HTML is broken or the field doesn't exist on that book.
    We use str (not float) for prices because we want to keep the original
    format (e.g. "£51.77") — converting to float risks precision loss.
    """
    url: str                          # full URL to the book's detail page
    title: str                        # book title from <h1>
    upc: Optional[str] = None         # Universal Product Code (unique ID)
    product_type: Optional[str] = None
    price_excl_tax: Optional[str] = None  # price before tax, e.g. "£51.77"
    price_incl_tax: Optional[str] = None  # price after tax
    tax: Optional[str] = None
    availability: Optional[str] = None   # e.g. "In stock (22 available)"
    num_reviews: Optional[str] = None    # number of customer reviews
    rating: Optional[str] = None         # "One", "Two", "Three", "Four", "Five"
    genre: Optional[str] = None          # from breadcrumb navigation

    def to_dict(self) -> dict:
        """Convert to plain dict for JSON/CSV/SQLite storage."""
        return asdict(self)


# ===========================================================================
# BooksListingParser — extracts book URLs from catalogue pages
# ===========================================================================
class BooksListingParser:
    """
    Parses a catalogue listing page and returns the URLs of individual books.

    INPUT:  HTML of http://books.toscrape.com/catalogue/page-1.html
    OUTPUT: list of absolute URLs like
            ["http://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html",
             "http://books.toscrape.com/catalogue/tipping-the-velvet_999/index.html",
             ...]

    WHY separate from BooksDetailParser?
      Single Responsibility: one class does one thing.
      Listing pages and detail pages have completely different HTML structures.
      Mixing both parsers in one class would make it hard to test and extend.
    """

    def parse(self, html: str, page_url: str) -> list[str]:
        """
        Find all book-detail links on a catalogue listing page.

        HOW IT WORKS:
          The listing page contains article elements like:
            <article class="product_pod">
              <div class="image_container">
                <a href="../a-light-in-the-attic_1000/index.html">
                  <img ...>
                </a>
              </div>
              ...
            </article>

          We find all <a> tags inside <article class="product_pod"> elements.
          Each href is a relative URL that we convert to absolute with urljoin().

        Args:
            html:     Raw HTML of the listing page.
            page_url: The URL this HTML came from (needed for urljoin).

        Returns:
            List of absolute URLs to individual book detail pages.
        """
        soup = BeautifulSoup(html, "lxml")
        book_urls = []

        # Find all article elements with class "product_pod"
        for article in soup.find_all("article", class_="product_pod"):
            # Each article has exactly one <a> inside image_container
            link_tag = article.find("a")
            if link_tag and link_tag.get("href"):
                relative_href = link_tag["href"]

                # urljoin resolves "../a-light_1000/index.html" relative to
                # "http://books.toscrape.com/catalogue/page-1.html"
                # giving us "http://books.toscrape.com/catalogue/a-light_1000/index.html"
                absolute_url = urljoin(page_url, relative_href)
                book_urls.append(absolute_url)

        logger.debug("Listing page %s → %d book URLs", page_url, len(book_urls))
        return book_urls

    def has_next_page(self, html: str, page_url: str) -> Optional[str]:
        """
        Check if there is a "next" pagination link and return its absolute URL.
        Returns None if we're on the last page.

        The next-page button looks like:
          <li class="next"><a href="page-2.html">next</a></li>
        """
        soup = BeautifulSoup(html, "lxml")
        next_li = soup.find("li", class_="next")
        if next_li:
            next_a = next_li.find("a")
            if next_a and next_a.get("href"):
                return urljoin(page_url, next_a["href"])
        return None  # no next page = we're done


# ===========================================================================
# BooksDetailParser — extracts book fields from a detail page
# ===========================================================================
class BooksDetailParser:
    """
    Parses a single book detail page and returns a Book dataclass instance.

    INPUT:  HTML of e.g.
            http://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html
    OUTPUT: Book instance with all 7 required fields filled in

    THE KEY TABLE:
      The detail page has a <table class="table table-striped"> where
      each row is <tr><th>Field Name</th><td>Value</td></tr>.
      We parse this into a dict: {"UPC": "a897fe39...", "Tax": "£0.00", ...}
      Then we look up each field by its header text.
    """

    # Mapping from the table's <th> text to our Book field name.
    # This is the ONLY place where we define the mapping — if the site
    # changes a header name, we update only this dict.
    FIELD_MAP = {
        "UPC":                 "upc",
        "Product Type":        "product_type",
        "Price (excl. tax)":   "price_excl_tax",
        "Price (incl. tax)":   "price_incl_tax",
        "Tax":                 "tax",
        "Availability":        "availability",
        "Number of reviews":   "num_reviews",
    }

    def parse(self, html: str, url: str) -> Book:
        """
        Parse one book detail page and return a Book instance.

        STEPS:
          1. Extract title from <h1>
          2. Parse the product info table into a dict
          3. Map table values to Book fields
          4. Extract rating from CSS class name
          5. Extract genre from breadcrumb navigation

        WHY try/except around each field?
          If one field's HTML is broken or missing, we still want
          all the other fields. We store None for missing fields
          rather than crashing the whole scrape.
        """
        soup = BeautifulSoup(html, "lxml")

        # ── Title ─────────────────────────────────────────────────────
        # <div class="product_main"><h1>A Light in the Attic</h1>...</div>
        title = "Unknown"
        try:
            h1 = soup.find("h1")
            if h1:
                title = h1.get_text(strip=True)
        except Exception as exc:
            logger.warning("Title extraction failed for %s: %s", url, exc)

        book = Book(url=url, title=title)

        # ── Product info table ─────────────────────────────────────────
        # Parse all <tr> rows into a flat dict: {"UPC": "abc123", ...}
        try:
            table_data = self._parse_table(soup)
            # Map table header names to Book fields using FIELD_MAP
            for header, field_name in self.FIELD_MAP.items():
                if header in table_data:
                    setattr(book, field_name, table_data[header])
        except Exception as exc:
            logger.warning("Table parsing failed for %s: %s", url, exc)

        # ── Star rating ────────────────────────────────────────────────
        # <p class="star-rating Three"> — the rating is in the CSS class!
        # We look for <p> that has "star-rating" class, then find the
        # extra class that is NOT "star-rating" — that's the word rating.
        try:
            rating_p = soup.find("p", class_="star-rating")
            if rating_p:
                classes = rating_p.get("class", [])
                # classes = ["star-rating", "Three"]
                # We want "Three", not "star-rating"
                rating_words = [c for c in classes if c != "star-rating"]
                if rating_words:
                    book.rating = rating_words[0]  # "One", "Two", ..., "Five"
        except Exception as exc:
            logger.warning("Rating extraction failed for %s: %s", url, exc)

        # ── Genre from breadcrumb ──────────────────────────────────────
        # Breadcrumb: Home > Mystery > Book Title
        # <ul class="breadcrumb">
        #   <li><a href="/">Home</a></li>
        #   <li><a href="/catalogue/category/books/mystery_3/index.html">Mystery</a></li>
        #   <li class="active">A Light in the Attic</li>
        # </ul>
        try:
            breadcrumb = soup.find("ul", class_="breadcrumb")
            if breadcrumb:
                links = breadcrumb.find_all("a")
                # links[0] = "Home", links[1] = genre category
                if len(links) >= 2:
                    book.genre = links[-1].get_text(strip=True)
        except Exception as exc:
            logger.warning("Genre extraction failed for %s: %s", url, exc)

        return book

    def _parse_table(self, soup: BeautifulSoup) -> dict[str, str]:
        """
        Parse the product info table into a plain dict.

        The table structure:
          <table class="table table-striped">
            <tbody>
              <tr>
                <th>UPC</th>
                <td>a897fe39b1053632</td>
              </tr>
              <tr>
                <th>Product Type</th>
                <td>Books</td>
              </tr>
              ...
            </tbody>
          </table>

        Returns: {"UPC": "a897fe39b1053632", "Product Type": "Books", ...}
        """
        result = {}
        table = soup.find("table", class_="table-striped")
        if not table:
            return result

        for row in table.find_all("tr"):
            th = row.find("th")  # column header (field name)
            td = row.find("td")  # column value
            if th and td:
                key = th.get_text(strip=True)    # e.g. "UPC"
                value = td.get_text(strip=True)  # e.g. "a897fe39b1053632"
                result[key] = value

        return result


# ===========================================================================
# BooksScraper — orchestrates the full scrape
# ===========================================================================
class BooksScraper:
    """
    The main scraper class. Ties together:
      - AsyncCrawler for parallel HTTP fetching
      - BooksListingParser for discovering book URLs
      - BooksDetailParser for extracting book data
      - DataStorage for saving results

    CRAWL STRATEGY:
      Phase 1 — Discover:
        Fetch all catalogue/page-*.html listing pages sequentially
        (there are only 50 of them, and we need to follow pagination).
        Collect all 1000 book-detail URLs.

      Phase 2 — Scrape:
        Fetch all book-detail pages in PARALLEL (up to max_concurrent at once).
        Parse each one into a Book instance.
        Save each Book to storage as it completes.

    This two-phase approach is cleaner than trying to crawl listing and
    detail pages simultaneously.
    """

    def __init__(
        self,
        max_concurrent: int = 10,
        max_books: int = 1000,        # set lower to scrape only a sample
        requests_per_second: float = 2.0,
        storage=None,                 # Optional DataStorage instance
    ) -> None:
        """
        Args:
            max_concurrent:      How many book detail pages to fetch at once.
            max_books:           Stop after this many books (1000 = all books).
            requests_per_second: Politeness throttle.
            storage:             Where to save results. None = results only in memory.
        """
        self.max_books = max_books
        self._storage = storage

        # Import here to keep the import chain explicit
        from crawler import AsyncCrawler
        from rate_limiter import RateLimiter

        # AsyncCrawler handles the HTTP layer
        self._crawler = AsyncCrawler(
            max_concurrent=max_concurrent,
            connect_timeout=10.0,
            read_timeout=30.0,
            max_retries=2,
            respect_robots=False,    # books.toscrape.com is a practice site,
                                     # it has no robots.txt restrictions
        )

        # Rate limiter: polite 2 req/sec, no per-domain needed (single site)
        self._rate_limiter = RateLimiter(
            requests_per_second=requests_per_second,
            per_domain=False,  # single site, global throttle is fine
            min_delay=0.3,
            jitter=0.2,
        )

        self._listing_parser = BooksListingParser()
        self._detail_parser = BooksDetailParser()

        # Results stored here; also saved to storage if provided
        self.books: list[Book] = []
        self.failed_urls: list[str] = []

    # ------------------------------------------------------------------
    # Phase 1: Discover all book URLs from pagination
    # ------------------------------------------------------------------

    async def _discover_book_urls(self) -> list[str]:
        """
        Walk through all catalogue listing pages and collect book-detail URLs.

        We follow the "next page" link sequentially because:
          - There are only 50 pages — not worth parallelising
          - Sequential is simpler to reason about
          - It respects the natural pagination flow

        Returns a deduplicated list of up to max_books book-detail URLs.
        """
        all_book_urls: list[str] = []
        current_url = BASE_URL + "catalogue/page-1.html"

        logger.info("Phase 1: discovering book URLs from listing pages...")

        while current_url and len(all_book_urls) < self.max_books:
            # Polite delay before each listing page request
            await self._rate_limiter.acquire(current_url)

            result = await self._crawler.fetch_url(current_url)
            if not result.success:
                logger.warning("Failed to fetch listing page: %s", current_url)
                break

            # Extract book URLs from this listing page
            page_book_urls = self._listing_parser.parse(result.content, current_url)
            all_book_urls.extend(page_book_urls)

            logger.info("  Page %s → %d books found (total: %d)",
                        current_url.split("/")[-1], len(page_book_urls), len(all_book_urls))

            # Find the next page link (None = we're on the last page)
            current_url = self._listing_parser.has_next_page(result.content, current_url)

        # Trim to max_books and deduplicate
        unique_urls = list(dict.fromkeys(all_book_urls))
        return unique_urls[:self.max_books]

    # ------------------------------------------------------------------
    # Phase 2: Scrape individual book detail pages
    # ------------------------------------------------------------------

    async def _scrape_book(self, url: str) -> Optional[Book]:
        """
        Fetch and parse one book detail page.
        Returns a Book instance on success, None on failure.

        This coroutine is called once per book URL.
        Many of these run concurrently (controlled by AsyncCrawler's semaphore).
        """
        await self._rate_limiter.acquire(url)
        result = await self._crawler.fetch_url(url)

        if not result.success:
            logger.warning("Failed to fetch book: %s — %s", url, result.error)
            self.failed_urls.append(url)
            return None

        book = self._detail_parser.parse(result.content, url)
        logger.debug("Scraped: [%s] %s | £%s | %s",
                     book.rating, book.title[:40], book.price_excl_tax, book.genre)
        return book

    async def _scrape_books_parallel(self, book_urls: list[str]) -> None:
        """
        Fetch all book detail pages in parallel using asyncio.gather().

        asyncio.gather() runs all _scrape_book() coroutines concurrently.
        AsyncCrawler's internal semaphore limits the actual concurrency.

        Results arrive in the SAME ORDER as book_urls because gather()
        preserves order even though fetches complete in arbitrary order.
        """
        logger.info("Phase 2: scraping %d book detail pages in parallel...", len(book_urls))

        # Create all coroutines (not started yet — just created)
        tasks = [self._scrape_book(url) for url in book_urls]

        # Run all tasks concurrently, get results in order
        results: list[Optional[Book]] = await asyncio.gather(*tasks)

        for book in results:
            if book is not None:
                self.books.append(book)
                # Save to storage immediately if configured
                if self._storage:
                    try:
                        await self._storage.save(book.to_dict())
                    except Exception as exc:
                        logger.error("Storage save failed for %s: %s", book.url, exc)

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------

    async def scrape(self) -> list[Book]:
        """
        Run the full two-phase scrape and return a list of Book instances.

        Usage:
            scraper = BooksScraper(max_books=50)
            books = await scraper.scrape()
            await scraper.close()
        """
        logger.info("Starting books.toscrape.com scrape (max_books=%d)", self.max_books)

        # Phase 1: find all book detail page URLs
        book_urls = await self._discover_book_urls()
        logger.info("Discovered %d book URLs", len(book_urls))

        # Phase 2: scrape all detail pages in parallel
        await self._scrape_books_parallel(book_urls)

        logger.info(
            "Scrape complete — %d books scraped | %d failed",
            len(self.books), len(self.failed_urls),
        )
        return self.books

    def print_sample(self, n: int = 5) -> None:
        """Print the first n books in a readable table format."""
        print(f"\n{'='*75}")
        print(f"  Sample results ({min(n, len(self.books))} of {len(self.books)} books)")
        print(f"{'='*75}")
        header = f"{'TITLE':<35} {'PRICE':>8} {'RATING':<6} {'AVAIL':>6} {'UPC'}"
        print(header)
        print("-" * 75)
        for book in self.books[:n]:
            price = book.price_excl_tax or "?"
            rating = book.rating or "?"
            avail = book.availability or "?"
            # Availability might be "In stock (22 available)" — extract number
            avail_num = re.search(r"\d+", avail)
            avail_str = avail_num.group() + " left" if avail_num else avail
            print(f"{book.title[:34]:<35} {price:>8} {rating:<6} {avail_str:>6}  {book.upc}")
        print("=" * 75)

    def get_stats(self) -> dict:
        """Return scraping statistics."""
        genres = {}
        ratings = {}
        for book in self.books:
            if book.genre:
                genres[book.genre] = genres.get(book.genre, 0) + 1
            if book.rating:
                ratings[book.rating] = ratings.get(book.rating, 0) + 1

        return {
            "total_scraped":   len(self.books),
            "total_failed":    len(self.failed_urls),
            "genres_found":    len(genres),
            "top_genres":      dict(sorted(genres.items(), key=lambda x: -x[1])[:5]),
            "rating_distribution": ratings,
        }

    async def close(self) -> None:
        """Close the HTTP session and storage backend."""
        await self._crawler.close()
        if self._storage:
            await self._storage.close()
        logger.info("BooksScraper closed.")

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()
