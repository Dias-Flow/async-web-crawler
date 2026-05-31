"""
async_crawler/test_books_scraper.py

Tests for the books.toscrape.com scraper.
All tests use fake HTML — no real HTTP requests.

Run with:
    pytest test_books_scraper.py -v
"""

import pytest
from books_scraper import BooksListingParser, BooksDetailParser, Book

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fake HTML that mirrors the real site structure
# ---------------------------------------------------------------------------

LISTING_HTML = """
<!DOCTYPE html>
<html>
<body>
<div class="page">
  <div class="page_inner">
    <article class="product_pod">
      <div class="image_container">
        <a href="a-light-in-the-attic_1000/index.html">
          <img src="..." alt="A Light in the Attic">
        </a>
      </div>
      <p class="star-rating Three"></p>
      <h3><a href="a-light-in-the-attic_1000/index.html" title="A Light in the Attic">A Light ...</a></h3>
      <p class="price_color">£51.77</p>
      <p class="instock availability">In stock</p>
    </article>

    <article class="product_pod">
      <div class="image_container">
        <a href="tipping-the-velvet_999/index.html">
          <img src="..." alt="Tipping the Velvet">
        </a>
      </div>
      <p class="star-rating One"></p>
      <h3><a href="tipping-the-velvet_999/index.html" title="Tipping the Velvet">Tipping ...</a></h3>
      <p class="price_color">£53.74</p>
    </article>
  </div>

  <!-- Pagination: there IS a next page -->
  <ul class="pager">
    <li class="next"><a href="page-2.html">next</a></li>
  </ul>
</div>
</body>
</html>
"""

LISTING_LAST_PAGE_HTML = """
<html><body>
  <article class="product_pod">
    <a href="../last-book_1/index.html"><img></a>
  </article>
  <!-- No next page link -->
</body></html>
"""

DETAIL_HTML = """
<!DOCTYPE html>
<html>
<body>
<div class="product_main">
  <h1>A Light in the Attic</h1>
  <p class="star-rating Three"></p>
  <p class="price_color">£51.77</p>
</div>

<ul class="breadcrumb">
  <li><a href="/">Home</a></li>
  <li><a href="/catalogue/category/books/poetry_23/index.html">Poetry</a></li>
  <li class="active">A Light in the Attic</li>
</ul>

<table class="table table-striped">
  <tbody>
    <tr><th>UPC</th>                 <td>a897fe39b1053632</td></tr>
    <tr><th>Product Type</th>        <td>Books</td></tr>
    <tr><th>Price (excl. tax)</th>   <td>£51.77</td></tr>
    <tr><th>Price (incl. tax)</th>   <td>£51.77</td></tr>
    <tr><th>Tax</th>                 <td>£0.00</td></tr>
    <tr><th>Availability</th>        <td>In stock (22 available)</td></tr>
    <tr><th>Number of reviews</th>   <td>0</td></tr>
  </tbody>
</table>
</body>
</html>
"""

BROKEN_DETAIL_HTML = """
<html><body>
  <h1>Broken Book</h1>
  <!-- No table, no breadcrumb, no rating -->
</body></html>
"""

LISTING_URL  = "http://books.toscrape.com/catalogue/page-1.html"
DETAIL_URL   = "http://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html"


# ===========================================================================
# BooksListingParser
# ===========================================================================
class TestBooksListingParser:

    def test_finds_book_urls(self):
        """Should return absolute URLs for all book articles on the page."""
        parser = BooksListingParser()
        urls = parser.parse(LISTING_HTML, LISTING_URL)
        assert len(urls) == 2

    def test_urls_are_absolute(self):
        """All returned URLs must start with http:// — no relative paths."""
        parser = BooksListingParser()
        urls = parser.parse(LISTING_HTML, LISTING_URL)
        for url in urls:
            assert url.startswith("http://"), f"Not absolute: {url}"

    def test_correct_urls_resolved(self):
        """Check that relative href is correctly resolved against the listing URL."""
        parser = BooksListingParser()
        urls = parser.parse(LISTING_HTML, LISTING_URL)
        # "a-light-in-the-attic_1000/index.html" relative to
        # "http://books.toscrape.com/catalogue/page-1.html"
        # should become:
        assert "http://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html" in urls
        assert "http://books.toscrape.com/catalogue/tipping-the-velvet_999/index.html" in urls

    def test_has_next_page_when_next_exists(self):
        """has_next_page should return the next page URL when pagination exists."""
        parser = BooksListingParser()
        next_url = parser.has_next_page(LISTING_HTML, LISTING_URL)
        assert next_url is not None
        # "page-2.html" relative to "http://books.toscrape.com/catalogue/page-1.html"
        assert "page-2.html" in next_url

    def test_has_next_page_returns_none_on_last_page(self):
        """On the last page (no next link), should return None."""
        parser = BooksListingParser()
        next_url = parser.has_next_page(LISTING_LAST_PAGE_HTML, LISTING_URL)
        assert next_url is None

    def test_empty_page_returns_empty_list(self):
        """No articles = empty list, not a crash."""
        parser = BooksListingParser()
        urls = parser.parse("<html><body></body></html>", LISTING_URL)
        assert urls == []


# ===========================================================================
# BooksDetailParser
# ===========================================================================
class TestBooksDetailParser:

    def test_title_extracted(self):
        parser = BooksDetailParser()
        book = parser.parse(DETAIL_HTML, DETAIL_URL)
        assert book.title == "A Light in the Attic"

    def test_upc_extracted(self):
        parser = BooksDetailParser()
        book = parser.parse(DETAIL_HTML, DETAIL_URL)
        assert book.upc == "a897fe39b1053632"

    def test_product_type_extracted(self):
        parser = BooksDetailParser()
        book = parser.parse(DETAIL_HTML, DETAIL_URL)
        assert book.product_type == "Books"

    def test_price_excl_tax_extracted(self):
        parser = BooksDetailParser()
        book = parser.parse(DETAIL_HTML, DETAIL_URL)
        assert book.price_excl_tax == "£51.77"

    def test_price_incl_tax_extracted(self):
        parser = BooksDetailParser()
        book = parser.parse(DETAIL_HTML, DETAIL_URL)
        assert book.price_incl_tax == "£51.77"

    def test_tax_extracted(self):
        parser = BooksDetailParser()
        book = parser.parse(DETAIL_HTML, DETAIL_URL)
        assert book.tax == "£0.00"

    def test_availability_extracted(self):
        parser = BooksDetailParser()
        book = parser.parse(DETAIL_HTML, DETAIL_URL)
        assert book.availability == "In stock (22 available)"

    def test_num_reviews_extracted(self):
        parser = BooksDetailParser()
        book = parser.parse(DETAIL_HTML, DETAIL_URL)
        assert book.num_reviews == "0"

    def test_rating_extracted(self):
        """Rating should be the word class, e.g. 'Three'."""
        parser = BooksDetailParser()
        book = parser.parse(DETAIL_HTML, DETAIL_URL)
        assert book.rating == "Three"

    def test_genre_extracted_from_breadcrumb(self):
        """Genre should come from the second breadcrumb link."""
        parser = BooksDetailParser()
        book = parser.parse(DETAIL_HTML, DETAIL_URL)
        assert book.genre == "Poetry"

    def test_url_stored(self):
        """The book's own URL must be stored in the Book instance."""
        parser = BooksDetailParser()
        book = parser.parse(DETAIL_HTML, DETAIL_URL)
        assert book.url == DETAIL_URL

    def test_broken_html_does_not_crash(self):
        """Parsing broken HTML should return a Book with None fields, not raise."""
        parser = BooksDetailParser()
        book = parser.parse(BROKEN_DETAIL_HTML, DETAIL_URL)
        assert isinstance(book, Book)
        assert book.title == "Broken Book"  # h1 is present in broken HTML
        assert book.upc is None             # table missing → None
        assert book.genre is None           # breadcrumb missing → None

    def test_to_dict_has_all_fields(self):
        """to_dict() must include all 7 required homework fields."""
        parser = BooksDetailParser()
        book = parser.parse(DETAIL_HTML, DETAIL_URL)
        d = book.to_dict()
        required = [
            "upc", "product_type", "price_excl_tax",
            "price_incl_tax", "tax", "availability", "num_reviews",
        ]
        for field in required:
            assert field in d, f"Missing field: {field}"


# ===========================================================================
# Book dataclass
# ===========================================================================
class TestBook:

    def test_to_dict_is_serialisable(self):
        """to_dict() output must be JSON-serialisable (no custom objects)."""
        import json
        book = Book(
            url="https://example.com",
            title="Test Book",
            upc="abc123",
            price_excl_tax="£9.99",
        )
        d = book.to_dict()
        json_str = json.dumps(d)  # must not raise
        assert "abc123" in json_str

    def test_optional_fields_default_to_none(self):
        """Fields not provided in constructor should be None, not raise AttributeError."""
        book = Book(url="https://x.com", title="X")
        assert book.upc is None
        assert book.tax is None
        assert book.num_reviews is None
