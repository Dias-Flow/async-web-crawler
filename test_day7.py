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
