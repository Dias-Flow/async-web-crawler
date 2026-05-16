"""
async_crawler/main_day4.py

Day 4 demo — shows rate limiting and robots.txt compliance in action.

Run with:
    python main_day4.py
"""

import asyncio
import time
from rate_limiter import RateLimiter, RobotsParser
import aiohttp


async def demo_rate_limiter() -> None:
    """
    Fire 6 requests through the rate limiter and print timing.
    We expect each to wait ~1 second between calls for the same domain.
    """
    print("\n── Rate Limiter Demo ──────────────────────────────")
    rl = RateLimiter(requests_per_second=1.0, min_delay=0.5, jitter=0.2)
    url = "https://example.com/page"

    t0 = time.perf_counter()
    for i in range(1, 7):
        waited = await rl.acquire(url)
        print(f"  Request {i}: waited {waited:.2f}s  (total elapsed {time.perf_counter()-t0:.2f}s)")

    print("\nRate limiter stats:", rl.get_stats())


async def demo_robots_parser() -> None:
    """
    Fetch robots.txt for a couple of domains and check whether
    specific paths are allowed.
    """
    print("\n── Robots.txt Demo ────────────────────────────────")
    rp = RobotsParser(user_agent="*")  # manages own session internally

    tests = [
        ("https://example.com", "https://example.com/"),
        ("https://httpbin.org", "https://httpbin.org/get"),
        ("https://httpbin.org", "https://httpbin.org/anything"),
    ]

    for base, check_url in tests:
        await rp.fetch_robots(base)           # new API: no session arg
        allowed = rp.can_fetch(check_url)     # new API: no rfp arg
        delay = rp.get_crawl_delay()          # new API: no rfp arg
        status = "✓ ALLOWED" if allowed else "✗ BLOCKED"
        print(f"  {status:12}  {check_url}  (crawl-delay={delay})")

    await rp.close()
    print("\nRobots parser stats:", rp.get_stats())


async def main() -> None:
    print("\n" + "=" * 60)
    print("  Day 4 Demo — Rate Limiting & robots.txt")
    print("=" * 60)
    await demo_rate_limiter()
    await demo_robots_parser()
    print()


if __name__ == "__main__":
    asyncio.run(main())
