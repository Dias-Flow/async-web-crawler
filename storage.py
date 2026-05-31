"""
async_crawler/storage.py  (Day 6)

Async storage backends: JSON Lines, CSV, and SQLite.

WHY THREE FORMATS?
  JSON Lines (.jsonl) — human-readable, easy to inspect with any text editor,
                        trivial to import into Python with json.loads().
  CSV                 — opens directly in Excel / Google Sheets.
                        Great for quick data analysis.
  SQLite              — a real database stored as one file.
                        Queryable with SQL, fast for large crawls,
                        survives crashes better than plain files.

WHY ASYNC I/O?
  If storage.save() blocked the event loop (like normal file.write()),
  ALL other crawling would pause while one page is being saved to disk.
  aiofiles and aiosqlite make writes non-blocking:
  the event loop continues fetching other pages while the OS writes to disk.

DESIGN — DataStorage abstract base class:
  Every backend implements the same two methods: save() and close().
  AsyncCrawler accepts any DataStorage object.
  You can swap JSON for SQLite without touching crawler code.
  You can use MultiStorage to write to ALL formats simultaneously.
"""

import asyncio
import csv
import io
import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import aiofiles
import aiosqlite

logger = logging.getLogger("DataStorage")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    """Current UTC time as ISO-8601 string, e.g. '2024-03-15T14:23:01+00:00'."""
    return datetime.now(timezone.utc).isoformat()


def _prepare_record_for_db(data: dict) -> dict:
    """
    Normalise a page_data dict from HTMLParser into a flat storage record.

    WHY normalise?
      HTMLParser returns 'links' as a Python list.
      Lists can't be stored as-is in CSV columns or SQLite TEXT columns.
      We JSON-encode them into strings: ["a", "b"] → '["a", "b"]'
      This way the data can be read back and decoded from any format.

    DB-specific schema: links/metadata serialised to JSON strings
    for SQLite TEXT columns. NOT used by JSONStorage or CSVStorage.
    """
    links    = data.get("links", [])
    metadata = data.get("metadata", {})

    return {
        "url":            data.get("url", ""),
        "title":          data.get("title") or "",
        "text":           data.get("text") or "",
        # Store lists and dicts as JSON strings so every backend can handle them
        "links_json":     json.dumps(links, ensure_ascii=False),
        "metadata_json":  json.dumps(metadata, ensure_ascii=False),
        "crawled_at":     data.get("crawled_at") or _now_iso(),
        "status_code":    data.get("status_code") or 200,
        "content_type":   data.get("content_type") or "text/html",
        "text_length":    data.get("text_length") or len(data.get("text") or ""),
        "links_count":    data.get("links_count") or len(links),
    }




def _add_defaults(data: dict) -> dict:
    """
    Minimal normalisation: ensure crawled_at is set.
    Links and metadata are kept as-is (list and dict) per Day-6 spec:
      {"links": list[str], "metadata": dict, ...}
    JSONStorage and CSVStorage call this; SQLiteStorage uses _prepare_record_for_db.
    """
    result = dict(data)
    if not result.get("crawled_at"):
        result["crawled_at"] = _now_iso()
    return result

# ===========================================================================
# Abstract base class
# ===========================================================================
class DataStorage(ABC):
    """
    Interface that all storage backends must implement.

    WHY abstract?
      Forces every subclass to implement save() and close().
      AsyncCrawler only knows about DataStorage — not about JSON or SQLite.
      This is the "Dependency Inversion" principle: high-level code (crawler)
      depends on an abstraction (DataStorage), not a concrete class.

    Usage:
        storage = JSONStorage("results.jsonl")
        crawler = AsyncCrawler(storage=storage)
        # crawler calls storage.save(page) after each page, storage.close() at shutdown
    """

    @abstractmethod
    async def save(self, data: dict) -> None:
        """Persist one page_data dict. Called after each page is crawled."""

    @abstractmethod
    async def close(self) -> None:
        """Flush all pending data and release file handles / DB connections."""

    def get_stats(self) -> dict:
        """Override to return backend-specific statistics."""
        return {}

    # Support 'async with storage:' syntax — useful in tests and demos
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()


# ===========================================================================
# JSONStorage — one JSON object per line (JSON Lines format)
# ===========================================================================
class JSONStorage(DataStorage):
    """
    Saves each page as one JSON object on its own line.

    WHY JSON Lines instead of a single JSON array?
      Option A — single array: [ {...}, {...}, {...} ]
        Appending requires: read whole file → add item → rewrite whole file.
        For 10,000 pages that means rewriting megabytes on every save. Slow.

      Option B — JSON Lines: one object per line (our choice)
        Appending just means: open file in append mode → write one line.
        O(1) per write regardless of file size.
        Each line is valid JSON on its own so you can read it line by line.

    Reading results back:
        import json
        with open("results.jsonl") as f:
            pages = [json.loads(line) for line in f]
    """

    def __init__(self, filepath: str) -> None:
        self._path = Path(filepath)
        self._saved_count: int = 0
        # Lock ensures only one coroutine writes at a time.
        # Without this, concurrent saves produce interleaved bytes
        # making JSON lines unparseable.
        self._lock = asyncio.Lock()

    async def save(self, data: dict) -> None:
        """
        Append one JSON record to the file.

        'async with aiofiles.open(...)' is the async equivalent of open().
        The write is non-blocking — the event loop can run other coroutines
        while the OS flushes bytes to disk.
        """
        # Save original data — links stays list, metadata stays dict,
        # matching the Day-6 standard structure exactly.
        record = _add_defaults(data)
        line = json.dumps(record, ensure_ascii=False, default=str)

        try:
            async with self._lock:  # one write at a time
                async with aiofiles.open(self._path, mode="a", encoding="utf-8") as f:
                    await f.write(line + "\n")
            self._saved_count += 1
            logger.debug("JSON saved: %s", record["url"])
        except OSError as exc:
            logger.error("JSON write error: %s", exc)

    async def close(self) -> None:
        logger.info("JSONStorage closed — %d records in %s", self._saved_count, self._path)

    def get_stats(self) -> dict:
        return {"format": "json_lines", "file": str(self._path), "saved": self._saved_count}


# ===========================================================================
# CSVStorage — spreadsheet-compatible format
# ===========================================================================
class CSVStorage(DataStorage):
    """
    Writes each page as one row in a CSV file.

    HEADER ROW:
      Written automatically on the very first save().
      If the file already exists (e.g. resuming a crashed crawl), we check
      its size — if non-zero, the header was already written and we skip it.

    WHY csv.writer + in-memory StringIO + aiofiles?
      csv.writer expects a regular (synchronous) file object.
      aiofiles gives us an async file object.
      The trick: write to a StringIO buffer first → get the CSV string → 
      write the string async. Two steps but fully non-blocking.

    WHY asyncio.Lock?
      Multiple coroutines might call save() simultaneously.
      Without a lock, two coroutines could interleave their writes:
        Coroutine A writes: "example.com/page1,"
        Coroutine B writes: "example.com/page2,"    ← mixed into A's row!
        Coroutine A finishes: "Title A\n"
      The lock ensures one complete row is written before the next starts.
    """

    # Headers are auto-detected from the first record's keys per Day-6 spec.
    # Complex types (lists, dicts) are JSON-serialised for CSV compatibility.

    def __init__(self, filepath: str) -> None:
        self._path = Path(filepath)
        self._header_written: bool = (
            self._path.exists() and self._path.stat().st_size > 0
        )
        self._saved_count: int = 0
        self._lock = asyncio.Lock()
        # Auto-detected from first record; None until first save()
        self._columns: list[str] = []

    async def save(self, data: dict) -> None:
        record = _add_defaults(data)
        # Serialise complex types to strings for CSV compatibility
        flat: dict = {}
        for k, v in record.items():
            if isinstance(v, (list, dict)):
                flat[k] = json.dumps(v, ensure_ascii=False)
            else:
                flat[k] = v if v is not None else ""

        # Auto-detect columns from first record
        if not self._columns:
            self._columns = list(flat.keys())

        row = [flat.get(col, "") for col in self._columns]

        buf = io.StringIO()
        writer = csv.writer(buf)
        if not self._header_written:
            writer.writerow(self._columns)  # auto-detected headers
        writer.writerow(row)
        csv_text = buf.getvalue()

        # Now write the string to disk asynchronously
        async with self._lock:
            try:
                async with aiofiles.open(
                    self._path, mode="a", encoding="utf-8", newline=""
                ) as f:
                    await f.write(csv_text)
                self._header_written = True
                self._saved_count += 1
                logger.debug("CSV saved: %s", record["url"])
            except OSError as exc:
                logger.error("CSV write error: %s", exc)

    async def close(self) -> None:
        logger.info("CSVStorage closed — %d rows in %s", self._saved_count, self._path)

    def get_stats(self) -> dict:
        return {"format": "csv", "file": str(self._path), "saved": self._saved_count}


# ===========================================================================
# SQLiteStorage — queryable database
# ===========================================================================
class SQLiteStorage(DataStorage):
    """
    Stores pages in a SQLite database file using aiosqlite.

    SQLITE vs plain files:
      - You can query: SELECT url, title FROM pages WHERE links_count > 10
      - You can update: no duplicate URLs (INSERT OR REPLACE handles restarts)
      - You can join: if you add multiple tables later
      - The file is a single .db file — easy to share

    BATCH INSERTS:
      Writing one row per INSERT transaction is slow in SQLite because
      each transaction flushes to disk (fsync).
      We buffer `batch_size` records in memory and write them ALL in ONE
      transaction. 20 rows in one transaction is ~20x faster than 20 single-row
      transactions.

      The buffer is flushed automatically when full, and force-flushed on close().
      If the program crashes before a flush, at most batch_size records are lost.

    INSERT OR REPLACE:
      If you crawl example.com/page twice (e.g. after a restart), the second
      INSERT silently replaces the first instead of creating a duplicate row.
      The UNIQUE constraint on the url column enforces this.
    """

    CREATE_TABLE_SQL = """
    CREATE TABLE IF NOT EXISTS pages (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        url           TEXT    UNIQUE NOT NULL,
        title         TEXT,
        text          TEXT,
        links_json    TEXT,
        metadata_json TEXT,
        crawled_at    TEXT,
        status_code   INTEGER,
        content_type  TEXT,
        text_length   INTEGER,
        links_count   INTEGER
    );
    CREATE INDEX IF NOT EXISTS idx_url        ON pages (url);
    CREATE INDEX IF NOT EXISTS idx_crawled_at ON pages (crawled_at);
    """
    # idx_url makes 'WHERE url = ?' fast
    # idx_crawled_at makes 'ORDER BY crawled_at' fast

    INSERT_SQL = """
    INSERT OR REPLACE INTO pages
        (url, title, text, links_json, metadata_json,
         crawled_at, status_code, content_type, text_length, links_count)
    VALUES
        (:url, :title, :text, :links_json, :metadata_json,
         :crawled_at, :status_code, :content_type, :text_length, :links_count)
    """
    # :name syntax is a named placeholder — aiosqlite replaces :url with dict["url"]
    # This is also SQL injection-safe: values are never interpolated as raw SQL

    def __init__(self, db_path: str, batch_size: int = 20) -> None:
        self._db_path = db_path
        self._batch_size = batch_size
        self._buffer: list[dict] = []       # records waiting to be written
        self._db: Optional[aiosqlite.Connection] = None
        self._lock = asyncio.Lock()
        self._saved_count: int = 0

    async def _connect(self) -> None:
        """Open DB and create tables on first use (lazy connection)."""
        if self._db is None:
            self._db = await aiosqlite.connect(self._db_path)
            # executescript runs multiple SQL statements at once (separated by ;)
            await self._db.executescript(self.CREATE_TABLE_SQL)
            await self._db.commit()
            logger.info("SQLiteStorage connected — %s", self._db_path)

    async def init_db(self) -> None:
        """Public Day-6 API: create/open the SQLite database and tables."""
        await self._connect()

    async def save(self, data: dict) -> None:
        """
        Buffer one record. Flush to DB when buffer reaches batch_size.

        The lock ensures that two coroutines don't flush simultaneously
        (which could cause duplicate writes or partial flushes).
        """
        await self._connect()
        record = _prepare_record_for_db(data)

        async with self._lock:
            self._buffer.append(record)
            if len(self._buffer) >= self._batch_size:
                await self._flush()   # buffer full → write now

    async def _flush(self) -> None:
        """
        Write all buffered records to SQLite in a single transaction.
        Must be called with self._lock already held.

        executemany() runs the INSERT for each item in self._buffer.
        commit() writes the whole batch to disk in one fsync.
        """
        if not self._buffer or self._db is None:
            return
        try:
            await self._db.executemany(self.INSERT_SQL, self._buffer)
            await self._db.commit()
            self._saved_count += len(self._buffer)
            logger.debug("SQLite flushed %d records", len(self._buffer))
            self._buffer.clear()
        except aiosqlite.Error as exc:
            logger.error("SQLite flush error: %s", exc)

    async def flush(self) -> None:
        """Public flush — call to force-write pending records without closing."""
        async with self._lock:
            await self._flush()

    async def close(self) -> None:
        """Flush remaining buffer and close DB connection."""
        if self._db:
            async with self._lock:
                await self._flush()   # don't lose the last batch!
            await self._db.close()
            self._db = None
        logger.info("SQLiteStorage closed — %d records in %s",
                    self._saved_count, self._db_path)

    async def query(self, sql: str, params: tuple = ()) -> list[dict]:
        """
        Run a SELECT and return rows as list of dicts.
        Used in demos to verify data was saved correctly.

        Example:
            rows = await db.query("SELECT url, title FROM pages LIMIT 5")
            for row in rows:
                print(row["url"], row["title"])
        """
        await self._connect()
        async with self._db.execute(sql, params) as cursor:
            # cursor.description = list of (name, ...) tuples for each column
            columns = [d[0] for d in cursor.description]
            rows = await cursor.fetchall()
        # zip(columns, row) pairs each column name with its value
        return [dict(zip(columns, row)) for row in rows]

    def get_stats(self) -> dict:
        return {
            "format":   "sqlite",
            "file":     self._db_path,
            "saved":    self._saved_count,
            "buffered": len(self._buffer),
        }


# ===========================================================================
# MultiStorage — fan-out writes to multiple backends
# ===========================================================================
class MultiStorage(DataStorage):
    """
    Saves to multiple backends simultaneously.

    Usage:
        storage = MultiStorage([
            JSONStorage("results.jsonl"),
            CSVStorage("results.csv"),
            SQLiteStorage("results.db"),
        ])
        # One save() call writes to all three

    asyncio.gather() runs all three saves concurrently — so saving to
    three backends takes roughly the same time as saving to one.

    return_exceptions=True means if one backend fails, the others still
    receive the data. The failure is logged, not raised.
    """

    def __init__(self, backends: list[DataStorage]) -> None:
        self._backends = backends

    async def save(self, data: dict) -> None:
        results = await asyncio.gather(
            *[b.save(data) for b in self._backends],
            return_exceptions=True,  # don't let one failure cancel the others
        )
        # Log any exceptions without crashing
        for backend, result in zip(self._backends, results):
            if isinstance(result, Exception):
                logger.error("%s save failed: %s", type(backend).__name__, result)

    async def close(self) -> None:
        await asyncio.gather(*[b.close() for b in self._backends])

    def get_stats(self) -> dict:
        """Returns a dict of backend_class_name → stats for each backend."""
        return {type(b).__name__: b.get_stats() for b in self._backends}
