"""
async_crawler/test_day5_day6.py

Tests for Day 5 (RetryStrategy) and Day 6 (Storage backends).
All offline — no real HTTP, no real file writes (using tmp_path).

Run with:
    pytest test_day5_day6.py -v
"""
import asyncio
import json
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, patch
import aiohttp

from retry_strategy import (
    RetryStrategy, TransientError, PermanentError,
    NetworkError, classify_aiohttp_error
)
from storage import JSONStorage, CSVStorage, SQLiteStorage, MultiStorage

pytestmark = pytest.mark.asyncio


# ===========================================================================
# classify_aiohttp_error
# ===========================================================================
class TestClassify:

    def test_404_is_permanent(self):
        exc = aiohttp.ClientResponseError(MagicMock(), (), status=404, message="Not Found")
        err = classify_aiohttp_error(exc)
        assert isinstance(err, PermanentError)

    def test_503_is_transient(self):
        exc = aiohttp.ClientResponseError(MagicMock(), (), status=503, message="Unavailable")
        err = classify_aiohttp_error(exc)
        assert isinstance(err, TransientError)

    def test_429_is_transient(self):
        exc = aiohttp.ClientResponseError(MagicMock(), (), status=429, message="Too Many")
        err = classify_aiohttp_error(exc)
        assert isinstance(err, TransientError)

    def test_timeout_is_transient(self):
        err = classify_aiohttp_error(asyncio.TimeoutError())
        assert isinstance(err, TransientError)

    def test_connector_error_is_network(self):
        exc = aiohttp.ClientConnectorError(MagicMock(), OSError("DNS failed"))
        err = classify_aiohttp_error(exc)
        assert isinstance(err, NetworkError)


# ===========================================================================
# RetryStrategy
# ===========================================================================
class TestRetryStrategy:

    async def test_success_first_try(self):
        """No retries needed when the function succeeds immediately."""
        retry = RetryStrategy(max_retries=3)
        async def ok(): return "result"
        result = await retry.execute_with_retry(ok)
        assert result == "result"
        assert retry.get_stats()["total_errors"] == 0

    async def test_permanent_error_not_retried(self):
        """PermanentError should raise immediately without retrying."""
        retry = RetryStrategy(max_retries=3, backoff_base=0.01)
        call_count = 0
        async def always_404():
            nonlocal call_count
            call_count += 1
            raise aiohttp.ClientResponseError(MagicMock(), (), status=404, message="NF")
        with pytest.raises(PermanentError):
            await retry.execute_with_retry(always_404, url="https://x.com/404")
        assert call_count == 1   # called exactly once, no retry

    async def test_transient_error_retried(self):
        """TransientError should be retried up to max_retries times."""
        retry = RetryStrategy(max_retries=2, backoff_base=0.01, backoff_factor=1.0)
        call_count = 0
        async def flaky():
            nonlocal call_count
            call_count += 1
            raise aiohttp.ClientResponseError(MagicMock(), (), status=503, message="")
        with pytest.raises(TransientError):
            await retry.execute_with_retry(flaky, url="https://x.com/503")
        assert call_count == 3   # original + 2 retries

    async def test_succeeds_on_retry(self):
        """Should return successfully if the function works on a later attempt."""
        retry = RetryStrategy(max_retries=3, backoff_base=0.01)
        call_count = 0
        async def succeeds_on_third():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise aiohttp.ClientResponseError(MagicMock(), (), status=503, message="")
            return "ok"
        result = await retry.execute_with_retry(succeeds_on_third)
        assert result == "ok"
        assert retry.get_stats()["successful_retries"] == 1

    async def test_exponential_backoff_increases(self):
        """Later attempts should wait longer than earlier ones."""
        retry = RetryStrategy(max_retries=3, backoff_base=1.0, backoff_factor=2.0)
        delays = [retry._backoff_for(i) for i in range(1, 4)]
        assert delays[0] < delays[1] < delays[2]

    async def test_max_backoff_cap(self):
        """No single wait should exceed max_backoff."""
        retry = RetryStrategy(max_retries=10, backoff_base=1.0, backoff_factor=10.0, max_backoff=5.0)
        for i in range(1, 11):
            assert retry._backoff_for(i) <= 5.0

    async def test_error_report_populated(self):
        """Error log should have one entry per failed attempt."""
        retry = RetryStrategy(max_retries=1, backoff_base=0.01)
        async def always_503():
            raise aiohttp.ClientResponseError(MagicMock(), (), status=503, message="")
        with pytest.raises(TransientError):
            await retry.execute_with_retry(always_503, url="https://test.com")
        report = retry.get_error_report()
        assert len(report) == 2   # attempt 1 + attempt 2 (1 retry)

    async def test_stats_error_counts(self):
        """stats error_counts should reflect error types seen."""
        retry = RetryStrategy(max_retries=0, backoff_base=0.01)
        async def perm():
            raise aiohttp.ClientResponseError(MagicMock(), (), status=404, message="")
        with pytest.raises(PermanentError):
            await retry.execute_with_retry(perm)
        stats = retry.get_stats()
        assert stats["error_counts"].get("PermanentError", 0) >= 1


# ===========================================================================
# JSONStorage
# ===========================================================================
class TestJSONStorage:

    async def test_save_creates_file(self, tmp_path):
        path = tmp_path / "out.jsonl"
        storage = JSONStorage(str(path))
        await storage.save({"url": "https://x.com", "title": "X", "text": "hello",
                            "links": [], "metadata": {}, "text_length": 5, "links_count": 0})
        await storage.close()
        assert path.exists()

    async def test_each_line_is_valid_json(self, tmp_path):
        path = tmp_path / "out.jsonl"
        storage = JSONStorage(str(path))
        for i in range(3):
            await storage.save({"url": f"https://x.com/{i}", "title": f"T{i}",
                                 "text": "", "links": [], "metadata": {},
                                 "text_length": 0, "links_count": 0})
        await storage.close()
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 3
        for line in lines:
            obj = json.loads(line)   # must not raise
            assert "url" in obj

    async def test_appends_not_overwrites(self, tmp_path):
        path = tmp_path / "out.jsonl"
        base = {"url": "https://x.com", "title": "T", "text": "",
                "links": [], "metadata": {}, "text_length": 0, "links_count": 0}
        for _ in range(2):
            s = JSONStorage(str(path))
            await s.save({**base})
            await s.close()
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 2   # two separate saves = two lines


# ===========================================================================
# CSVStorage
# ===========================================================================
class TestCSVStorage:

    async def test_header_written_once(self, tmp_path):
        path = tmp_path / "out.csv"
        storage = CSVStorage(str(path))
        for i in range(3):
            await storage.save({"url": f"https://x.com/{i}", "title": f"T{i}",
                                 "text": "", "links": [], "metadata": {},
                                 "text_length": 0, "links_count": 0})
        await storage.close()
        lines = path.read_text().strip().splitlines()
        assert len(lines) == 4   # 1 header + 3 rows

    async def test_url_appears_in_csv(self, tmp_path):
        path = tmp_path / "out.csv"
        storage = CSVStorage(str(path))
        await storage.save({"url": "https://unique-url.com", "title": "T",
                             "text": "", "links": [], "metadata": {},
                             "text_length": 0, "links_count": 0})
        await storage.close()
        content = path.read_text()
        assert "unique-url.com" in content


# ===========================================================================
# SQLiteStorage
# ===========================================================================
class TestSQLiteStorage:

    async def test_save_and_query(self, tmp_path):
        path = str(tmp_path / "test.db")
        storage = SQLiteStorage(path, batch_size=1)
        await storage.save({"url": "https://example.com", "title": "Example",
                             "text": "hello", "links": ["https://a.com"],
                             "metadata": {}, "text_length": 5, "links_count": 1})
        await storage.flush()
        rows = await storage.query("SELECT url, title FROM pages")
        await storage.close()
        assert len(rows) == 1
        assert rows[0]["url"] == "https://example.com"
        assert rows[0]["title"] == "Example"

    async def test_duplicate_url_replaced(self, tmp_path):
        path = str(tmp_path / "test.db")
        storage = SQLiteStorage(path, batch_size=1)
        rec = {"url": "https://example.com", "title": "V1", "text": "",
               "links": [], "metadata": {}, "text_length": 0, "links_count": 0}
        await storage.save(rec)
        await storage.flush()
        rec2 = {**rec, "title": "V2"}
        await storage.save(rec2)
        await storage.flush()
        rows = await storage.query("SELECT title FROM pages WHERE url='https://example.com'")
        await storage.close()
        assert len(rows) == 1
        assert rows[0]["title"] == "V2"

    async def test_batch_flush(self, tmp_path):
        path = str(tmp_path / "batch.db")
        storage = SQLiteStorage(path, batch_size=5)
        for i in range(12):
            await storage.save({"url": f"https://x.com/{i}", "title": f"T{i}",
                                 "text": "", "links": [], "metadata": {},
                                 "text_length": 0, "links_count": 0})
        await storage.flush()
        rows = await storage.query("SELECT COUNT(*) as n FROM pages")
        await storage.close()
        assert rows[0]["n"] == 12


# ===========================================================================
# MultiStorage
# ===========================================================================
class TestMultiStorage:

    async def test_saves_to_all_backends(self, tmp_path):
        json_path = str(tmp_path / "out.jsonl")
        csv_path  = str(tmp_path / "out.csv")
        db_path   = str(tmp_path / "out.db")

        multi = MultiStorage([
            JSONStorage(json_path),
            CSVStorage(csv_path),
            SQLiteStorage(db_path, batch_size=1),
        ])

        rec = {"url": "https://example.com", "title": "T", "text": "",
               "links": [], "metadata": {}, "text_length": 0, "links_count": 0}
        await multi.save(rec)
        await multi.close()

        assert Path(json_path).exists()
        assert Path(csv_path).exists()

        db = SQLiteStorage(db_path)
        rows = await db.query("SELECT url FROM pages")
        await db.close()
        assert rows[0]["url"] == "https://example.com"


# ---------------------------------------------------------------------------
from unittest.mock import MagicMock


# ===========================================================================
# JSONStorage concurrency test (Fix 3)
# ===========================================================================
class TestJSONStorageConcurrency:

    async def test_concurrent_saves_produce_valid_json_lines(self, tmp_path):
        """
        20 concurrent saves must each produce exactly one valid JSON line.
        Before the Lock fix, interleaved writes could corrupt lines.
        """
        path = str(tmp_path / "concurrent.jsonl")
        storage = JSONStorage(path)

        rec = {"url": "https://x.com", "title": "T", "text": "",
               "links": [], "metadata": {}, "text_length": 0, "links_count": 0}

        # Fire 20 saves at exactly the same time
        await asyncio.gather(*[
            storage.save({**rec, "url": f"https://x.com/{i}"})
            for i in range(20)
        ])
        await storage.close()

        lines = Path(path).read_text().strip().splitlines()
        assert len(lines) == 20, f"Expected 20 lines, got {len(lines)}"
        for i, line in enumerate(lines):
            obj = json.loads(line)   # raises if line is corrupt
            assert "url" in obj

import json
from pathlib import Path
