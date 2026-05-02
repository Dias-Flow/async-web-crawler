"""
async_crawler/test_parser.py

Day 2 unit tests for HTMLParser and the fetch_and_parse integration.

All tests are offline — no real HTTP requests made.

Run with:
    pytest test_parser.py -v
"""

import pytest
from bs4 import BeautifulSoup

from parser import HTMLParser, _empty_page_data

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fixtures — reusable HTML strings
# ---------------------------------------------------------------------------

FULL_HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>Test Page</title>
  <meta name="description" content="A test description">
  <meta name="keywords" content="test, html, parser">
  <meta property="og:title" content="OG Title">
</head>
<body>
  <h1>Main Heading</h1>
  <h2>Sub Heading</h2>
  <h3>Small Heading</h3>
  <p>This is a paragraph with some text.</p>
  <a href="/relative">Relative link</a>
  <a href="https://external.com/page">External link</a>
  <a href="https://example.com/absolute">Absolute link</a>
  <a href="mailto:x@x.com">Mail link (should be skipped)</a>
  <a href="javascript:void(0)">JS link (should be skipped)</a>
  <img src="/images/photo.jpg" alt="A photo">
  <img src="https://cdn.example.com/logo.png" alt="Logo">
  <img src="" alt="Empty src (should be skipped)">
  <table><tr><td>data</td></tr></table>
  <ul><li>item</li></ul>
  <ol><li>item</li></ol>
  <script>var x = 1;</script>
  <style>.foo { color: red; }</style>
</body>
</html>
"""

BROKEN_HTML = "<html><body><p>Unclosed tag <b>Bold text</body>"

MINIMAL_HTML = "<html><body><p>Just text.</p></body></html>"

BASE_URL = "https://example.com"


# ===========================================================================
# _empty_page_data
# ===========================================================================
class TestEmptyPageData:
    def test_shape(self):
        """_empty_page_data must contain all expected keys."""
        data = _empty_page_data("https://x.com")
        required_keys = {
            "url", "title", "text", "text_length", "links", "links_count",
            "images", "images_count", "headings", "metadata",
            "tables_count", "lists_count", "error",
        }
        assert required_keys.issubset(data.keys())

    def test_url_stored(self):
        data = _empty_page_data("https://x.com")
        assert data["url"] == "https://x.com"


# ===========================================================================
# HTMLParser.extract_metadata
# ===========================================================================
class TestExtractMetadata:
    def setup_method(self):
        self.parser = HTMLParser()

    def _soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    def test_title_extracted(self):
        soup = self._soup(FULL_HTML)
        meta = self.parser.extract_metadata(soup)
        assert meta["title"] == "Test Page"

    def test_description_extracted(self):
        soup = self._soup(FULL_HTML)
        meta = self.parser.extract_metadata(soup)
        assert meta["description"] == "A test description"

    def test_keywords_extracted(self):
        soup = self._soup(FULL_HTML)
        meta = self.parser.extract_metadata(soup)
        assert meta["keywords"] == "test, html, parser"

    def test_missing_title_returns_none(self):
        soup = self._soup(MINIMAL_HTML)
        meta = self.parser.extract_metadata(soup)
        assert meta["title"] is None

    def test_og_title_fallback(self):
        """If <title> is absent, og:title should be used."""
        html = '<head><meta property="og:title" content="OG Fallback"></head>'
        soup = self._soup(html)
        meta = self.parser.extract_metadata(soup)
        assert meta["title"] == "OG Fallback"


# ===========================================================================
# HTMLParser.extract_text
# ===========================================================================
class TestExtractText:
    def setup_method(self):
        self.parser = HTMLParser()

    def _soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    def test_text_extracted(self):
        soup = self._soup(FULL_HTML)
        text = self.parser.extract_text(soup)
        assert "This is a paragraph" in text

    def test_scripts_stripped(self):
        """Script content must NOT appear in extracted text."""
        soup = self._soup(FULL_HTML)
        text = self.parser.extract_text(soup)
        assert "var x = 1" not in text

    def test_styles_stripped(self):
        soup = self._soup(FULL_HTML)
        text = self.parser.extract_text(soup)
        assert ".foo" not in text

    def test_selector_scope(self):
        """When a CSS selector is provided, only text inside that element is returned."""
        html = "<body><nav>NAV TEXT</nav><main><p>MAIN TEXT</p></main></body>"
        soup = self._soup(html)
        text = self.parser.extract_text(soup, selector="main")
        assert "MAIN TEXT" in text
        assert "NAV TEXT" not in text

    def test_missing_selector_falls_back_to_whole_page(self):
        html = "<body><p>Hello</p></body>"
        soup = self._soup(html)
        text = self.parser.extract_text(soup, selector="article")
        assert "Hello" in text   # selector not found → whole page used


# ===========================================================================
# HTMLParser.extract_links
# ===========================================================================
class TestExtractLinks:
    def setup_method(self):
        self.parser = HTMLParser()

    def _soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    def test_relative_link_resolved(self):
        soup = self._soup(FULL_HTML)
        links = self.parser.extract_links(soup, BASE_URL)
        assert "https://example.com/relative" in links

    def test_absolute_link_preserved(self):
        soup = self._soup(FULL_HTML)
        links = self.parser.extract_links(soup, BASE_URL)
        assert "https://example.com/absolute" in links

    def test_external_link_included(self):
        soup = self._soup(FULL_HTML)
        links = self.parser.extract_links(soup, BASE_URL)
        assert "https://external.com/page" in links

    def test_mailto_skipped(self):
        soup = self._soup(FULL_HTML)
        links = self.parser.extract_links(soup, BASE_URL)
        assert not any("mailto" in l for l in links)

    def test_javascript_skipped(self):
        soup = self._soup(FULL_HTML)
        links = self.parser.extract_links(soup, BASE_URL)
        assert not any("javascript" in l for l in links)

    def test_no_duplicates(self):
        html = """
        <a href="/page">link</a>
        <a href="/page">duplicate</a>
        <a href="https://example.com/page">same absolute</a>
        """
        soup = self._soup(html)
        links = self.parser.extract_links(soup, BASE_URL)
        assert links.count("https://example.com/page") == 1

    def test_empty_page_returns_empty_list(self):
        soup = self._soup("<html></html>")
        links = self.parser.extract_links(soup, BASE_URL)
        assert links == []


# ===========================================================================
# HTMLParser.extract_images
# ===========================================================================
class TestExtractImages:
    def setup_method(self):
        self.parser = HTMLParser()

    def _soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    def test_relative_image_resolved(self):
        soup = self._soup(FULL_HTML)
        images = self.parser.extract_images(soup, BASE_URL)
        srcs = [img["src"] for img in images]
        assert "https://example.com/images/photo.jpg" in srcs

    def test_absolute_image_preserved(self):
        soup = self._soup(FULL_HTML)
        images = self.parser.extract_images(soup, BASE_URL)
        srcs = [img["src"] for img in images]
        assert "https://cdn.example.com/logo.png" in srcs

    def test_alt_text_captured(self):
        soup = self._soup(FULL_HTML)
        images = self.parser.extract_images(soup, BASE_URL)
        alts = [img["alt"] for img in images]
        assert "A photo" in alts

    def test_empty_src_skipped(self):
        soup = self._soup(FULL_HTML)
        images = self.parser.extract_images(soup, BASE_URL)
        srcs = [img["src"] for img in images]
        assert "" not in srcs


# ===========================================================================
# HTMLParser.extract_headings
# ===========================================================================
class TestExtractHeadings:
    def setup_method(self):
        self.parser = HTMLParser()

    def _soup(self, html: str) -> BeautifulSoup:
        return BeautifulSoup(html, "html.parser")

    def test_h1_extracted(self):
        soup = self._soup(FULL_HTML)
        headings = self.parser.extract_headings(soup)
        assert "Main Heading" in headings["h1"]

    def test_h2_extracted(self):
        soup = self._soup(FULL_HTML)
        headings = self.parser.extract_headings(soup)
        assert "Sub Heading" in headings["h2"]

    def test_h3_extracted(self):
        soup = self._soup(FULL_HTML)
        headings = self.parser.extract_headings(soup)
        assert "Small Heading" in headings["h3"]

    def test_empty_page_returns_empty_lists(self):
        soup = self._soup(MINIMAL_HTML)
        headings = self.parser.extract_headings(soup)
        assert headings == {"h1": [], "h2": [], "h3": []}


# ===========================================================================
# HTMLParser.parse_html — top-level integration
# ===========================================================================
class TestParseHtml:
    def setup_method(self):
        self.parser = HTMLParser()

    async def test_full_parse_returns_all_keys(self):
        result = await self.parser.parse_html(FULL_HTML, BASE_URL)
        for key in ("url", "title", "text", "text_length", "links", "links_count",
                    "images", "images_count", "headings", "metadata",
                    "tables_count", "lists_count"):
            assert key in result, f"Missing key: {key}"

    async def test_tables_and_lists_counted(self):
        result = await self.parser.parse_html(FULL_HTML, BASE_URL)
        assert result["tables_count"] == 1
        assert result["lists_count"] == 2   # one <ul> and one <ol>

    async def test_broken_html_does_not_raise(self):
        """BeautifulSoup handles broken HTML — parse_html must never raise."""
        result = await self.parser.parse_html(BROKEN_HTML, BASE_URL)
        assert result is not None
        assert result["error"] is None        # broken HTML is still parseable

    async def test_empty_html_sets_error(self):
        result = await self.parser.parse_html("", BASE_URL)
        assert result["error"] is not None

    async def test_url_is_preserved(self):
        result = await self.parser.parse_html(MINIMAL_HTML, BASE_URL)
        assert result["url"] == BASE_URL

    async def test_text_length_matches_text(self):
        result = await self.parser.parse_html(FULL_HTML, BASE_URL)
        assert result["text_length"] == len(result["text"])

    async def test_links_count_matches_links(self):
        result = await self.parser.parse_html(FULL_HTML, BASE_URL)
        assert result["links_count"] == len(result["links"])


# ===========================================================================
# AsyncCrawler.fetch_and_parse — integration test with mocked HTTP
# ===========================================================================
class TestFetchAndParse:
    async def test_fetch_and_parse_returns_structured_dict(self):
        """
        Mock the HTTP layer and verify fetch_and_parse returns a parsed dict,
        not a raw FetchResult.
        """
        from unittest.mock import AsyncMock, MagicMock, patch
        from crawler import AsyncCrawler

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.text = AsyncMock(return_value=FULL_HTML)
        mock_resp.raise_for_status = MagicMock()

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        async with AsyncCrawler(max_retries=0) as crawler:
            with patch.object(crawler._session, "get", return_value=mock_ctx):
                result = await crawler.fetch_and_parse(BASE_URL)

        # Must be a dict, not a FetchResult
        assert isinstance(result, dict)
        assert result["url"] == BASE_URL
        assert result["title"] == "Test Page"
        assert result["links_count"] > 0
        assert result["fetch_elapsed"] >= 0

    async def test_fetch_failure_returns_error_dict(self):
        """
        If the HTTP request fails, fetch_and_parse should return an error dict,
        not raise an exception.
        """
        import aiohttp
        from unittest.mock import AsyncMock, MagicMock, patch
        from crawler import AsyncCrawler

        mock_ctx = AsyncMock()
        mock_ctx.__aenter__ = AsyncMock(
            side_effect=aiohttp.ClientResponseError(
                request_info=MagicMock(), history=(), status=404, message="Not Found"
            )
        )
        mock_ctx.__aexit__ = AsyncMock(return_value=False)

        async with AsyncCrawler(max_retries=0) as crawler:
            with patch.object(crawler._session, "get", return_value=mock_ctx):
                result = await crawler.fetch_and_parse("https://example.com/missing")

        assert isinstance(result, dict)
        assert result["error"] is not None
        assert result["links"] == []
