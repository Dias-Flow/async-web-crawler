# Async Web Crawler — Days 1–7

A modular asynchronous web crawler built with Python asyncio + aiohttp.

## Quick start

```bash
# 1. Create and activate virtualenv (do this once)
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run demos
python main.py           # Day 1: parallel HTTP fetch benchmark
python main_day2.py      # Day 2: HTML parsing + JSON output
python main_day3.py      # Day 3: site crawl with queue + depth control
python main_day4.py      # Day 4: rate limiting + robots.txt demo
python main_day5.py      # Day 5: retry strategy demo
python main_day6.py      # Day 6: all three storage backends
python main_day7.py      # Day 7: AdvancedCrawler full integration

# 4. Run all tests
pytest -v

# 5. CLI usage
python advanced_crawler.py --urls https://example.com --max-pages 30 --report report.html
python advanced_crawler.py --config config.yaml --report report.html
```

## File map

| File | Day | Purpose |
|------|-----|---------|
| `crawler.py` | 1–4 | AsyncCrawler: HTTP, semaphore, retries, crawl loop |
| `parser.py` | 2 | HTMLParser: links, text, metadata, headings, images |
| `queue_manager.py` | 3 | CrawlerQueue + SemaphoreManager |
| `rate_limiter.py` | 4 | RateLimiter + RobotsParser |
| `retry_strategy.py` | 5 | RetryStrategy + error taxonomy |
| `storage.py` | 6 | JSONStorage, CSVStorage, SQLiteStorage, MultiStorage |
| `advanced_crawler.py` | 7 | AdvancedCrawler, CrawlerStats, SitemapParser, CLI |
| `config.yaml` | 7 | Example YAML configuration |

## Running individual test files

```bash
pytest test_crawler.py   -v   # Day 1
pytest test_parser.py    -v   # Day 2
pytest test_day3_day4.py -v   # Days 3–4
pytest test_day5_day6.py -v   # Days 5–6
pytest test_day7.py      -v   # Day 7
```
