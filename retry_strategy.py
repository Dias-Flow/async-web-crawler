"""
async_crawler/retry_strategy.py  (Day 5)

Reliable retry logic with exponential back-off and error classification.

THE PROBLEM THIS SOLVES:
  The internet is unreliable. A server might:
    - be temporarily overloaded (503) → worth retrying in a few seconds
    - not have the page (404) → retrying is pointless
    - be rate-limiting us (429) → retry, but wait much longer
    - have a DNS failure → retry, infrastructure might recover

  Day 1's crawler had a simple "retry on network error" loop baked in.
  Day 5 makes this a proper, configurable, testable system with:
    - A taxonomy of error types (Transient / Permanent / Network / Parse)
    - Exponential back-off (wait longer after each failure)
    - Full error logging and statistics
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Any, Optional, Type

import aiohttp

logger = logging.getLogger("RetryStrategy")


# ===========================================================================
# Error taxonomy — four types of failures
# ===========================================================================

class CrawlerError(Exception):
    """
    Base class for all crawler errors.
    We use our OWN exception hierarchy instead of aiohttp's because:
      1. Our code should not depend on aiohttp internals everywhere
      2. We can add crawler-specific data (URL, attempt number) to each error
      3. Tests can raise CrawlerError without needing aiohttp installed
    """

class TransientError(CrawlerError):
    """
    Temporary failure — WORTH RETRYING.
    The server is alive but currently struggling.
    Examples: 503 Service Unavailable, 429 Too Many Requests, read timeout.
    Strategy: retry with exponential back-off.
    """

class PermanentError(CrawlerError):
    """
    Deterministic failure — DO NOT RETRY.
    The server gave a clear "no" answer that won't change on the next attempt.
    Examples: 404 Not Found, 403 Forbidden, 401 Unauthorized.
    Strategy: skip this URL, record the failure, move on.
    """

class NetworkError(CrawlerError):
    """
    Low-level connectivity problem — WORTH RETRYING.
    These are infrastructure issues, not application-level responses.
    Examples: DNS lookup failure, connection refused, TCP reset.
    Strategy: retry — the problem may fix itself.
    """

class ParseError(CrawlerError):
    """
    The page was fetched but its content could not be processed.
    Retrying a parse error rarely helps (the page content won't change).
    Strategy: treat as permanent, don't retry.
    """


# ---------------------------------------------------------------------------
# HTTP status code → error class mapping
# ---------------------------------------------------------------------------
# This dict is the single place where HTTP codes become meaningful types.
# Any code not listed here falls back to:
#   4xx → PermanentError
#   5xx → TransientError
HTTP_STATUS_MAP: dict[int, Type[CrawlerError]] = {
    400: PermanentError,   # Bad Request — our request is malformed, won't help to retry
    401: PermanentError,   # Unauthorized — need credentials we don't have
    403: PermanentError,   # Forbidden — server says "no" explicitly
    404: PermanentError,   # Not Found — page doesn't exist
    405: PermanentError,   # Method Not Allowed
    410: PermanentError,   # Gone — stronger 404, page was deleted permanently
    429: TransientError,   # Too Many Requests — retry but wait much longer
    500: TransientError,   # Internal Server Error — might be a fluke, retry
    502: TransientError,   # Bad Gateway — upstream server error, might recover
    503: TransientError,   # Service Unavailable — server is overloaded, retry
    504: TransientError,   # Gateway Timeout — retry
}


def classify_aiohttp_error(exc: Exception) -> CrawlerError:
    """
    Translate a raw aiohttp/asyncio exception into our error taxonomy.

    This is the "translation layer" between the HTTP library and our code.
    After this function, the rest of RetryStrategy only deals with
    CrawlerError subclasses and doesn't need to know about aiohttp internals.
    """
    if isinstance(exc, aiohttp.ClientResponseError):
        # Server responded with an HTTP error code
        cls = HTTP_STATUS_MAP.get(exc.status)
        if cls is None:
            # Unknown code: use 4xx/5xx rule
            cls = PermanentError if exc.status < 500 else TransientError
        return cls(f"HTTP {exc.status}: {exc.message}")

    if isinstance(exc, asyncio.TimeoutError):
        # Server did not respond in time — might recover
        return TransientError("Request timed out")

    if isinstance(exc, aiohttp.ClientConnectorError):
        # Could not establish TCP connection (DNS fail, port closed, etc.)
        return NetworkError(f"Connection failed: {exc}")

    if isinstance(exc, aiohttp.ClientError):
        # Any other aiohttp error (SSL, encoding, etc.)
        return NetworkError(f"Network error: {exc}")

    # Unknown exception type — treat as transient (try at least one more time)
    return TransientError(f"Unknown error: {exc}")


# ===========================================================================
# ErrorRecord — one entry in the error log
# ===========================================================================
@dataclass
class ErrorRecord:
    """
    Snapshot of one failed attempt, stored in the error log.

    WHY store records and not just increment counters?
      Records let us build detailed reports:
        "URL https://x.com/page failed 3 times: TimeoutError, TimeoutError, HTTP 503"
      Just counters would only tell us "3 TimeoutErrors total across all URLs".
    """
    url: str
    attempt: int           # 1 = first try, 2 = first retry, etc.
    error_type: str        # "TransientError", "PermanentError", etc.
    message: str           # human-readable error description
    timestamp: float = field(default_factory=time.time)
    next_retry_in: Optional[float] = None  # seconds until next attempt


# ===========================================================================
# RetryStrategy
# ===========================================================================
class RetryStrategy:
    """
    Wraps any async function and retries it intelligently on failure.

    EXPONENTIAL BACK-OFF explained:
      Each retry waits LONGER than the previous one.
      Formula: wait = backoff_base × (backoff_factor ^ attempt_number)

      With backoff_base=1.0 and backoff_factor=2.0:
        Attempt 1 fails → wait 1 × 2^1 = 2 seconds
        Attempt 2 fails → wait 1 × 2^2 = 4 seconds
        Attempt 3 fails → wait 1 × 2^3 = 8 seconds

      WHY exponential?
        If a server is struggling, hammering it with retries every second
        makes things worse. Exponential back-off gives it breathing room.
        Most transient failures resolve within a few seconds to a few minutes.

    SPECIAL CASE — 429 Too Many Requests:
      This explicitly means "slow down". We add a longer penalty_429 sleep
      (default 30s) instead of the normal back-off.
    """

    def __init__(
        self,
        max_retries: int = 3,
        backoff_base: float = 1.0,
        backoff_factor: float = 2.0,
        max_backoff: float = 60.0,
        retry_on: Optional[list[Type[CrawlerError]]] = None,
        penalty_429: float = 30.0,
    ) -> None:
        """
        Args:
            max_retries:    Extra attempts after the first failure. 3 = up to 4 total tries.
            backoff_base:   Base seconds for wait calculation.
            backoff_factor: Multiplier applied each attempt. 2.0 = doubles each time.
            max_backoff:    Cap on any single sleep. Prevents waiting hours on high attempt counts.
            retry_on:       Error classes that trigger a retry. Defaults to Transient + Network.
            penalty_429:    Extra seconds to sleep when the server says "too many requests".
        """
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self.backoff_factor = backoff_factor
        self.max_backoff = max_backoff
        # tuple() makes isinstance() checks faster than list checks
        self.retry_on = tuple(retry_on or [TransientError, NetworkError])
        self.penalty_429 = penalty_429

        # Error log: one ErrorRecord per failed attempt
        self._error_log: list[ErrorRecord] = []
        self._total_attempts: int = 0
        self._successful_retries: int = 0  # "failed then recovered" count

    def _backoff_for(self, attempt: int) -> float:
        """
        Calculate the sleep duration before retry attempt number `attempt`.
        Capped at max_backoff so we never wait more than e.g. 60 seconds.
        """
        raw = self.backoff_base * (self.backoff_factor ** attempt)
        return min(raw, self.max_backoff)

    def _should_retry(self, error: CrawlerError) -> bool:
        """True if this error type is in our retry-on list."""
        return isinstance(error, self.retry_on)

    async def execute_with_retry(
        self,
        coro_fn: Callable,    # the async function to call
        *args,                # positional args forwarded to coro_fn
        url: str = "",        # for logging/statistics only
        **kwargs,             # keyword args forwarded to coro_fn
    ) -> Any:
        """
        Execute an async function, retrying on transient failures.

        USAGE:
            result = await retry.execute_with_retry(
                crawler.fetch_url,
                "https://example.com",
                url="https://example.com",   # optional, for logs
            )

        FLOW:
          attempt 0: call coro_fn() → success → return immediately
          attempt 0: call coro_fn() → PermanentError → raise immediately (no retry)
          attempt 0: call coro_fn() → TransientError → sleep → attempt 1
          attempt 1: call coro_fn() → success → return (and record "successful retry")
          attempt 1: call coro_fn() → TransientError → sleep → attempt 2
          ...
          attempt max_retries: call coro_fn() → TransientError → raise (exhausted)
        """
        last_error: Optional[CrawlerError] = None
        succeeded_on_retry = False

        # range(max_retries + 1): attempt 0 = first try, 1..max_retries = retries
        for attempt in range(self.max_retries + 1):
            self._total_attempts += 1
            try:
                result = await coro_fn(*args, **kwargs)

                # SUCCESS
                if attempt > 0:
                    # We failed at least once but eventually succeeded
                    self._successful_retries += 1
                    logger.info("✓ Recovered after %d retries — %s", attempt, url)

                return result

            except Exception as raw_exc:
                # Translate the raw exception to our taxonomy
                error = classify_aiohttp_error(raw_exc)

                record = ErrorRecord(
                    url=url,
                    attempt=attempt + 1,
                    error_type=type(error).__name__,
                    message=str(error),
                )

                if not self._should_retry(error):
                    # PermanentError or ParseError — log and re-raise immediately
                    self._error_log.append(record)
                    logger.warning(
                        "✗ Permanent error on attempt %d — %s: %s",
                        attempt + 1, url, error,
                    )
                    raise error

                if attempt >= self.max_retries:
                    # Retryable error but we've used all our retries
                    self._error_log.append(record)
                    logger.error(
                        "✗ Exhausted %d retries — %s: %s",
                        self.max_retries, url, error,
                    )
                    raise error

                # Calculate sleep before next attempt
                wait = self._backoff_for(attempt + 1)

                # 429 gets a longer penalty — the server explicitly asked us to slow down
                if isinstance(error, TransientError) and "429" in str(error):
                    wait = max(wait, self.penalty_429)

                record.next_retry_in = wait
                self._error_log.append(record)

                logger.warning(
                    "⚠ Attempt %d/%d failed — %s: %s — retrying in %.1fs",
                    attempt + 1, self.max_retries + 1, url, error, wait,
                )
                last_error = error
                await asyncio.sleep(wait)

        raise last_error  # type: ignore  (should be unreachable, but satisfies mypy)

    def get_stats(self) -> dict:
        """
        Summary of all errors seen during this crawl session.

        error_counts: how many of each type occurred (e.g. {"TransientError": 5})
        successful_retries: operations that failed at least once but eventually worked
        permanent_urls: list of URLs that hit a PermanentError (not retried)
        """
        from collections import Counter
        type_counts = Counter(r.error_type for r in self._error_log)
        permanent_urls = list(set(
            r.url for r in self._error_log
            if r.error_type == "PermanentError" and r.url
        ))
        return {
            "total_attempts":     self._total_attempts,
            "total_errors":       len(self._error_log),
            "successful_retries": self._successful_retries,
            "error_counts":       dict(type_counts),
            "permanent_urls":     permanent_urls,
        }

    def get_error_report(self) -> list[dict]:
        """
        Full error log as a list of plain dicts (JSON-serialisable).
        Used by Day 7 HTML report generator.
        """
        return [
            {
                "url":           r.url,
                "attempt":       r.attempt,
                "error_type":    r.error_type,
                "message":       r.message,
                "timestamp":     r.timestamp,
                "next_retry_in": r.next_retry_in,
            }
            for r in self._error_log
        ]
