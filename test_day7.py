"""
async_crawler/test_day7.py

Tests for Day 7: CrawlerStats, SitemapParser, HTML report, config loader.
All offline — no real HTTP.

Run with:
    pytest test_day7.py -v
"""
import json
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from advanced_crawler import CrawlerStats, export_to_html_report, load_config

pytestmark = pytest.mark.asyncio


# ===========================================================================
# CrawlerStats
# ===========================================================================
class TestCrawlerStats:

    def test_initial_state(self):
        s = CrawlerStats()
        assert s.total_pages == 0
        assert s.successful == 0
        assert s.failed == 0

    def test_record_page_increments(self):
        s = CrawlerStats()
        s.record_page({"url": "https://x.com", "fetch_elapsed": 0.5})
        assert s.successful == 1
        assert s.total_pages == 1

    def test_record_failure_increments(self):
        s = CrawlerStats()
        s.record_failure("https://x.com/404", "PermanentError: 404")
        assert s.failed == 1
        assert s.total_pages == 1

    def test_domain_counts(self):
        s = CrawlerStats()
        s.record_page({"url": "https://example.com/a", "fetch_elapsed": 0})
        s.record_page({"url": "https://example.com/b", "fetch_elapsed": 0})
        assert s.domain_counts["example.com"] == 2

    def test_pages_per_second(self):
        """Speed should be positive after recording pages."""
        s = CrawlerStats()
        for i in range(5):
            s.record_page({"url": f"https://x.com/{i}", "fetch_elapsed": 0.1})
        s.finish()
        assert s.pages_per_second >= 0

    def test_to_dict_keys(self):
        s = CrawlerStats()
        d = s.to_dict()
        for key in ("total_pages", "successful", "failed", "elapsed_seconds",
                    "pages_per_second", "avg_fetch_time", "status_codes",
                    "top_domains", "error_types"):
            assert key in d

    def test_avg_fetch_time(self):
        s = CrawlerStats()
        s.record_page({"url": "https://a.com", "fetch_elapsed": 1.0})
        s.record_page({"url": "https://b.com", "fetch_elapsed": 3.0})
        assert abs(s.avg_fetch_time - 2.0) < 0.01


# ===========================================================================
# HTML report
# ===========================================================================
class TestHTMLReport:

    def test_report_creates_file(self, tmp_path):
        s = CrawlerStats()
        s.record_page({"url": "https://example.com", "title": "Test",
                        "links_count": 3, "text_length": 500, "fetch_elapsed": 0.2})
        s.finish()
        out = str(tmp_path / "report.html")
        export_to_html_report(s, out, {"https://example.com": {
            "url": "https://example.com", "title": "Test",
            "links_count": 3, "text_length": 500,
        }})
        content = Path(out).read_text()
        assert "<html" in content
        assert "example.com" in content
        assert "Total" in content

    def test_report_contains_stats(self, tmp_path):
        s = CrawlerStats()
        s.record_page({"url": "https://x.com/p", "fetch_elapsed": 0})
        s.record_failure("https://x.com/e", "timeout")
        s.finish()
        out = str(tmp_path / "r.html")
        export_to_html_report(s, out, {})
        html = Path(out).read_text()
        # Should contain success and failure counts
        assert "1" in html   # at least one page


# ===========================================================================
# Config loader
# ===========================================================================
class TestLoadConfig:

    def test_load_json_config(self, tmp_path):
        cfg = {"max_pages": 42, "max_depth": 3, "start_urls": ["https://x.com"]}
        f = tmp_path / "config.json"
        f.write_text(json.dumps(cfg))
        loaded = load_config(str(f))
        assert loaded["max_pages"] == 42
        assert loaded["start_urls"] == ["https://x.com"]

    def test_load_yaml_config(self, tmp_path):
        yaml_text = "max_pages: 10\nmax_depth: 2\nstart_urls:\n  - https://y.com\n"
        f = tmp_path / "config.yaml"
        f.write_text(yaml_text)
        try:
            loaded = load_config(str(f))
            assert loaded["max_pages"] == 10
        except ImportError:
            pytest.skip("PyYAML not installed")

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            load_config(str(tmp_path / "nonexistent.yaml"))

# ===========================================================================
# HTML report escaping tests (mentor update 9)
# ===========================================================================
class TestHTMLReportEscaping:

    def test_report_escapes_untrusted_page_data(self, tmp_path):
        s = CrawlerStats()
        malicious_url = 'https://example.com/?q=" onclick="alert(1)'
        malicious_title = '<script>alert("xss")</script>'
        s.record_page({
            "url": malicious_url,
            "title": malicious_title,
            "links_count": 1,
            "text_length": 10,
            "fetch_elapsed": 0.1,
            "status_code": 200,
        })
        s.finish()

        out = str(tmp_path / "safe_report.html")
        export_to_html_report(s, out, {
            malicious_url: {
                "url": malicious_url,
                "title": malicious_title,
                "links_count": 1,
                "text_length": 10,
            }
        })

        html = Path(out).read_text(encoding="utf-8")
        assert "<script>" not in html
        assert "onclick=\"alert" not in html
        assert "&lt;script&gt;alert(&quot;xss&quot;)&lt;/script&gt;" in html
        assert "rel=\"noopener noreferrer\"" in html

# ===========================================================================
# AdvancedCrawler integration tests (mentor update 10)
# ===========================================================================
class _MemoryStorage:
    def __init__(self):
        self.saved = []
        self.closed = False

    async def save(self, data: dict) -> None:
        self.saved.append(data)

    async def close(self) -> None:
        self.closed = True


def _html(title: str, links: list[str] | None = None) -> str:
    links = links or []
    anchors = "".join(f'<a href="{url}">{url}</a>' for url in links)
    return f"<html><head><title>{title}</title></head><body><h1>{title}</h1>{anchors}</body></html>"


class TestAdvancedCrawlerIntegration:

    async def test_crawl_allows_links_from_all_seed_domains(self):
        """same_domain_only=True must allow every domain from start_urls, not only the first."""
        from advanced_crawler import AdvancedCrawler
        from crawler import FetchResult

        pages = {
            "https://alpha.test/": _html("Alpha", [
                "https://alpha.test/a",
                "https://outside.test/blocked",
            ]),
            "https://beta.test/": _html("Beta", [
                "https://beta.test/b",
            ]),
            "https://alpha.test/a": _html("Alpha child"),
            "https://beta.test/b": _html("Beta child"),
        }
        storage = _MemoryStorage()
        crawler = AdvancedCrawler(
            start_urls=["https://alpha.test/", "https://beta.test/"],
            max_pages=4,
            max_depth=1,
            same_domain_only=True,
            respect_robots=False,
            storage=storage,
            requests_per_second=1000,
            min_delay=0,
            jitter=0,
        )

        async def fake_fetch(url: str):
            return FetchResult(
                url=url,
                status=200,
                content=pages[url],
                content_type="text/html",
                elapsed=0.01,
            )

        crawler._crawler._do_fetch_raising = fake_fetch
        try:
            results = await crawler.crawl()
        finally:
            await crawler.close()

        assert "https://alpha.test/a" in results
        assert "https://beta.test/b" in results
        assert "https://outside.test/blocked" not in results
        assert len(storage.saved) == len(results)

    async def test_crawl_adds_sitemap_urls_to_queue(self):
        """When use_sitemap=True, sitemap URLs should be added to the crawl input."""
        from advanced_crawler import AdvancedCrawler
        from crawler import FetchResult

        pages = {
            "https://site.test/": _html("Seed"),
            "https://site.test/from-sitemap": _html("From sitemap"),
        }
        crawler = AdvancedCrawler(
            start_urls=["https://site.test/"],
            max_pages=2,
            max_depth=0,
            use_sitemap=True,
            respect_robots=False,
            requests_per_second=1000,
            min_delay=0,
            jitter=0,
        )

        async def fake_fetch(url: str):
            return FetchResult(url=url, status=200, content=pages[url], content_type="text/html", elapsed=0.01)

        crawler._crawler._do_fetch_raising = fake_fetch
        with patch("advanced_crawler.SitemapParser.discover_sitemap_urls", new=AsyncMock(return_value=["https://site.test/from-sitemap"])):
            try:
                results = await crawler.crawl()
            finally:
                await crawler.close()

        assert "https://site.test/from-sitemap" in results

    async def test_crawl_retries_transient_fetch_errors_and_exposes_retry_stats(self):
        """AdvancedCrawler.crawl() should use RetryStrategy and expose retry stats in get_stats()."""
        import aiohttp
        from advanced_crawler import AdvancedCrawler
        from crawler import FetchResult

        crawler = AdvancedCrawler(
            start_urls=["https://retry.test/"],
            max_pages=1,
            max_depth=0,
            max_retries=1,
            respect_robots=False,
            requests_per_second=1000,
            min_delay=0,
            jitter=0,
        )
        crawler._retry.backoff_base = 0.01
        crawler._retry.backoff_factor = 1.0
        calls = 0

        async def flaky_fetch(url: str):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise aiohttp.ClientResponseError(MagicMock(), (), status=503, message="Service Unavailable")
            return FetchResult(url=url, status=200, content=_html("Recovered"), content_type="text/html", elapsed=0.01)

        crawler._crawler._do_fetch_raising = flaky_fetch
        try:
            results = await crawler.crawl()
            stats = crawler.get_stats()
        finally:
            await crawler.close()

        assert calls == 2
        assert "https://retry.test/" in results
        assert stats["retry"]["successful_retries"] == 1
        assert stats["retry"]["error_counts"].get("TransientError", 0) >= 1
        assert "progress" in stats
        assert "percent" in stats["progress"]
        assert "eta_seconds" in stats["progress"]

    async def test_cli_run_from_args_uses_advanced_crawler(self, tmp_path):
        """CLI scenario: run_from_args should build and run AdvancedCrawler without direct user code."""
        from advanced_crawler import run_from_args, build_arg_parser

        created = {}

        class _Stats:
            def print_summary(self):
                created["summary_printed"] = True

        class _FakeCrawler:
            def __init__(self, **kwargs):
                created["kwargs"] = kwargs
                self.stats = _Stats()

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                created["closed"] = True

            async def crawl(self):
                created["crawled"] = True
                return {}

            def export_to_html_report(self, output_path: str):
                Path(output_path).write_text("<html></html>", encoding="utf-8")
                created["report"] = output_path

        report = tmp_path / "report.html"
        args = build_arg_parser().parse_args([
            "--urls", "https://cli.test/",
            "--max-pages", "3",
            "--max-depth", "1",
            "--rate-limit", "5",
            "--report", str(report),
            "--no-robots",
        ])

        with patch("advanced_crawler.AdvancedCrawler", _FakeCrawler):
            await run_from_args(args)

        assert created["crawled"] is True
        assert created["closed"] is True
        assert created["summary_printed"] is True
        assert created["kwargs"]["start_urls"] == ["https://cli.test/"]
        assert created["kwargs"]["max_pages"] == 3
        assert report.exists()
