"""
async_crawler/main_day5.py  — Day 5 demo: retry strategy in action.
Run with: python main_day5.py
"""
import asyncio
from retry_strategy import RetryStrategy, TransientError, NetworkError, classify_aiohttp_error
import aiohttp

async def simulate_flaky_endpoint():
    """
    Demonstrate RetryStrategy.execute_with_retry() against httpbin endpoints
    that intentionally return error codes.
    """
    print("\n" + "=" * 55)
    print("  Day 5 Demo — Retry Strategy")
    print("=" * 55)

    retry = RetryStrategy(max_retries=2, backoff_base=0.5, backoff_factor=2.0)

    async with aiohttp.ClientSession() as session:
        # ── Test 1: 503 (transient) should be retried ─────────────────
        print("\n[1] Fetching /status/503  — expect 2 retries then failure")
        try:
            async def fetch_503():
                async with session.get("https://httpbin.org/status/503") as r:
                    r.raise_for_status()
            await retry.execute_with_retry(fetch_503, url="https://httpbin.org/status/503")
        except Exception as e:
            print(f"    Final error (expected): {e}")

        # ── Test 2: 404 (permanent) should NOT be retried ─────────────
        print("\n[2] Fetching /status/404  — expect immediate failure, no retry")
        try:
            async def fetch_404():
                async with session.get("https://httpbin.org/status/404") as r:
                    r.raise_for_status()
            await retry.execute_with_retry(fetch_404, url="https://httpbin.org/status/404")
        except Exception as e:
            print(f"    Final error (expected): {e}")

        # ── Test 3: 200 OK — no retry needed ──────────────────────────
        print("\n[3] Fetching /get  — expect immediate success")
        try:
            async def fetch_ok():
                async with session.get("https://httpbin.org/get") as r:
                    r.raise_for_status()
                    return await r.json()
            result = await retry.execute_with_retry(fetch_ok, url="https://httpbin.org/get")
            print(f"    Success: url={result.get('url')}")
        except Exception as e:
            print(f"    Unexpected error: {e}")

    print("\nError report:")
    for rec in retry.get_error_report():
        print(f"  [{rec['error_type']}] attempt {rec['attempt']} — {rec['message']}")

    print("\nStats:", retry.get_stats())

asyncio.run(simulate_flaky_endpoint())
