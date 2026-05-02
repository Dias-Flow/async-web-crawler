"""
async_crawler/parser.py  (Day 2)

HTML parsing — extracts structured data from raw HTML strings.

WHY A SEPARATE FILE FROM crawler.py?
  Single Responsibility Principle: one class does one thing.
  crawler.py knows HOW to GET a page.
  parser.py knows HOW TO READ a page.
  Neither needs to know the other's internals.
  This also makes testing easier: you can test HTMLParser with fake HTML
  strings without needing an HTTP server.

LIBRARY CHOICE:
  BeautifulSoup4 (bs4) — the industry standard for Python HTML parsing.
  We prefer the lxml parser backend (faster C library).
  We fall back to html.parser (Python stdlib) if lxml is not installed.
"""

import logging
import re
from urllib.parse import urljoin, urlparse
from typing import Optional

from bs4 import BeautifulSoup, FeatureNotFound

logger = logging.getLogger("HTMLParser")


# ---------------------------------------------------------------------------
# _empty_page_data — the "zero value" result dict
# ---------------------------------------------------------------------------
def _empty_page_data(url: str) -> dict:
    """
    Returns a dict with ALL expected keys set to their zero/empty values.

    WHY this function?
      Every code path in the project must return the same dict shape.
      Without this, callers would crash on KeyError when parsing fails:
          page["links_count"]  → KeyError if parsing failed halfway

      With this function, even a completely failed parse returns a valid dict.
      Callers can safely do page["links_count"] and get 0, not a crash.

      It's also a contract: "here are ALL the keys you can expect."
    """
    return {
        "url":          url,
        "title":        None,
        "text":         "",
        "text_length":  0,
        "links":        [],
        "links_count":  0,
        "images":       [],
        "images_count": 0,
        "headings":     {"h1": [], "h2": [], "h3": []},
        "metadata":     {"title": None, "description": None, "keywords": None},
        "tables_count": 0,
        "lists_count":  0,
        "error":        None,
    }


# ===========================================================================
# HTMLParser
# ===========================================================================
class HTMLParser:
    """
    Stateless HTML parser. Takes raw HTML strings, returns structured dicts.

    STATELESS means: no instance variables change after __init__.
    The same HTMLParser instance can safely parse pages concurrently
    because it doesn't store any per-parse state.
    Each parse_html() call is completely independent.
    """

    async def parse_html(self, html: str, url: str) -> dict:
        """
        Main entry point. Parse a raw HTML string and return a full data dict.

        WHY async?
          The method signature is async so AsyncCrawler can call it with 'await'
          consistently. Currently it's all CPU work (no I/O), but in the future
          you could move heavy parsing to a thread pool with asyncio.to_thread()
          without changing any callers.

        STRUCTURE: build BeautifulSoup once, then call each extractor.
        Each extractor is in its own try/except.
        If extract_images() crashes, extract_links() still runs.
        You always get partial results rather than nothing.
        """
        result = _empty_page_data(url)

        if not html:
            result["error"] = "Empty HTML string received"
            logger.warning("parse_html called with empty string for %s", url)
            return result

        # Build the parse tree
        # lxml is a C library — 10-50x faster than Python's html.parser
        try:
            soup = BeautifulSoup(html, "lxml")
        except FeatureNotFound:
            # lxml not installed — fall back to stdlib parser (slower but works)
            logger.warning("lxml not found, falling back to html.parser (slower)")
            soup = BeautifulSoup(html, "html.parser")
        except Exception as exc:
            result["error"] = f"BeautifulSoup init error: {exc}"
            logger.error("Failed to build soup for %s: %s", url, exc)
            return result

        # Run all extractors — each is independent, each has its own try/except
        try:
            result["metadata"] = self.extract_metadata(soup)
            result["title"] = result["metadata"]["title"]
        except Exception as exc:
            logger.warning("Metadata extraction failed for %s: %s", url, exc)

        try:
            result["text"] = self.extract_text(soup)
            result["text_length"] = len(result["text"])
        except Exception as exc:
            logger.warning("Text extraction failed for %s: %s", url, exc)

        try:
            result["links"] = self.extract_links(soup, url)
            result["links_count"] = len(result["links"])
        except Exception as exc:
            logger.warning("Link extraction failed for %s: %s", url, exc)

        try:
            result["images"] = self.extract_images(soup, url)
            result["images_count"] = len(result["images"])
        except Exception as exc:
            logger.warning("Image extraction failed for %s: %s", url, exc)

        try:
            result["headings"] = self.extract_headings(soup)
        except Exception as exc:
            logger.warning("Heading extraction failed for %s: %s", url, exc)

        try:
            result["tables_count"] = len(soup.find_all("table"))
            result["lists_count"] = len(soup.find_all(["ul", "ol"]))
        except Exception as exc:
            logger.warning("Table/list count failed for %s: %s", url, exc)

        return result

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """
        Find all <a href="..."> links and return them as absolute URLs.

        WHY convert to absolute URLs?
          Raw HTML often contains relative links like '/about' or '../faq'.
          A crawler needs absolute URLs to fetch the next page.
          urljoin() does the conversion:
            urljoin("https://example.com/blog/post", "/about")
            → "https://example.com/about"
            urljoin("https://example.com/blog/post", "../faq")
            → "https://example.com/faq"

        WHY skip mailto:, javascript:, #anchor?
          These are not fetchable web pages.
          mailto:user@example.com → opens email client, not a webpage
          javascript:void(0) → runs JS, not a URL
          #section → same-page anchor, no new request needed

        WHY deduplicate?
          A page might link to /about from nav, footer, and body.
          We only want to visit /about once.
          'seen' set gives O(1) duplicate check.
        """
        seen: set[str] = set()
        links: list[str] = []

        for tag in soup.find_all("a", href=True):
            raw_href: str = tag["href"].strip()

            # Skip non-HTTP link types
            if not raw_href or raw_href.startswith(("#", "javascript:", "mailto:", "tel:")):
                continue

            # Convert relative → absolute URL
            absolute = urljoin(base_url, raw_href)

            # Validate and deduplicate
            if self._is_valid_url(absolute) and absolute not in seen:
                seen.add(absolute)
                links.append(absolute)

        return links

    def extract_text(
        self, soup: BeautifulSoup, selector: Optional[str] = None
    ) -> str:
        """
        Extract all human-readable text from the page (or a section of it).

        WHY remove <script> and <style> tags first?
          soup.get_text() would include JavaScript code and CSS rules
          in the text output. That's noise, not content.
          tag.decompose() removes the tag AND its content from the tree.

        WHY the optional selector?
          Many pages have a lot of nav/footer/sidebar text that isn't the
          main content. If you pass selector="article" or selector="main",
          you get just the article body text.
          This is useful for text length analysis and search indexing.

        WHY re.sub(r"\s+", " ")?
          soup.get_text() produces lots of whitespace: tabs, multiple
          newlines, etc. This regex replaces any whitespace sequence with
          a single space, giving clean readable text.
        """
        scope = soup  # default: use the entire page

        if selector:
            found = soup.select_one(selector)
            if found:
                scope = found
            else:
                logger.debug("Selector '%s' matched nothing, using whole page", selector)

        # Remove script and style elements — they're code, not text content
        for tag in scope.find_all(["script", "style", "noscript"]):
            tag.decompose()

        # get_text(separator=" ") puts a space between adjacent tags
        # so "<b>Hello</b><b>World</b>" becomes "Hello World" not "HelloWorld"
        raw_text = scope.get_text(separator=" ")

        # Collapse any sequence of whitespace (spaces, tabs, newlines) into one space
        cleaned = re.sub(r"\s+", " ", raw_text).strip()
        return cleaned

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        """
        Extract the page's self-description from <head> meta tags.

        WHAT WE EXTRACT:
          <title>         → the browser tab title, usually the page headline
          meta description → a short summary of the page (used by Google)
          meta keywords   → comma-separated topic tags (old SEO technique)
          og:title / og:description → Open Graph tags used for social sharing

        Open Graph tags are Facebook/Twitter's standard for "how should this
        page appear when shared on social media?" They're often more reliable
        than the raw <title> tag.

        WHY check og: as fallback?
          Some pages set og:title but not <title>, or vice versa.
          Having both checks gives better coverage.
        """
        meta: dict = {"title": None, "description": None, "keywords": None}

        # <title>Page Title</title>
        title_tag = soup.find("title")
        if title_tag and title_tag.string:
            meta["title"] = title_tag.string.strip()

        # Open Graph title as fallback: <meta property="og:title" content="...">
        if not meta["title"]:
            og_title = soup.find("meta", property="og:title")
            if og_title:
                meta["title"] = og_title.get("content", "").strip() or None

        # <meta name="description" content="...">
        desc_tag = soup.find("meta", attrs={"name": "description"})
        if desc_tag:
            meta["description"] = desc_tag.get("content", "").strip() or None

        if not meta["description"]:
            og_desc = soup.find("meta", property="og:description")
            if og_desc:
                meta["description"] = og_desc.get("content", "").strip() or None

        # <meta name="keywords" content="python, crawler, async">
        kw_tag = soup.find("meta", attrs={"name": "keywords"})
        if kw_tag:
            meta["keywords"] = kw_tag.get("content", "").strip() or None

        return meta

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        """
        Find all <img> tags and return their absolute src URLs + alt text.

        WHY collect images?
          Image counts are useful crawl statistics.
          Alt text is important for accessibility analysis.
          Image URLs can be used to download images later.

        WHY skip empty src?
          Some lazy-loaded images use data-src instead of src:
          <img data-src="/photo.jpg" src="">
          The real URL is in data-src. Handling that is an advanced topic
          (requires JavaScript rendering). For now we skip empty src.
        """
        images: list[dict] = []

        for img in soup.find_all("img", src=True):
            raw_src: str = img["src"].strip()
            if not raw_src:
                continue  # skip lazy-loaded images with empty src

            # Relative → absolute URL (same logic as links)
            absolute_src = urljoin(base_url, raw_src)
            if not self._is_valid_url(absolute_src):
                continue  # skip data: URIs, blob: URLs, etc.

            images.append({
                "src": absolute_src,
                "alt": img.get("alt", "").strip(),  # "" if no alt attribute
            })

        return images

    def extract_headings(self, soup: BeautifulSoup) -> dict:
        """
        Collect all h1, h2, h3 headings from the page.

        WHY headings?
          Headings are the outline of a page. They reveal what the page is
          about without needing to read all the body text.
          h1 is usually the page title, h2 are main sections, h3 are subsections.

        Returns: {"h1": ["Main Title"], "h2": ["Section 1", "Section 2"], "h3": [...]}
        """
        headings: dict = {"h1": [], "h2": [], "h3": []}

        for level in ("h1", "h2", "h3"):
            for tag in soup.find_all(level):
                text = tag.get_text(separator=" ").strip()
                if text:
                    headings[level].append(text)

        return headings

    @staticmethod
    def _is_valid_url(url: str) -> bool:
        """
        Check that a URL is safe to enqueue for fetching.

        We only want http:// and https:// URLs.
        We explicitly reject:
          data:image/png;base64,...  → inline image data, not fetchable
          blob:https://...           → browser-internal URLs
          //example.com/            → protocol-relative (valid but we want explicit)
          #fragment                  → same-page anchor, no new request

        urlparse() safely handles malformed URLs without raising exceptions.
        """
        try:
            parsed = urlparse(url)
            return parsed.scheme in ("http", "https") and bool(parsed.netloc)
        except Exception:
            return False
