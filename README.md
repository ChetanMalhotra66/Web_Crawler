# DarkTrace Crawling Engine

Two layers:
- **Crawler/** — the single-source Scrapy spider. Fetches one authorized URL,
  routes through Tor/I2P/direct based on the domain, parses it, hashes and
  stores the content, and logs a crawl-result record. This is the unit of
  work; it doesn't know about other sources or scheduling.
- **engine/** — the queue-driven layer on top. Reads a multi-source registry
  (`sources.yaml`), decides what's due, and dispatches crawl jobs across
  Redis-backed Celery queues (fast/standard/bulk), with job-level retry,
  dead-lettering, and per-source crawl state. This is what makes "single
  source" into "many sources, on schedule, reliably."

## Setup

### 1. Create & Activate Virtual Environment

On Windows (PowerShell):
python -m venv venv
.\venv\Scripts\Activate.ps1
*(If PowerShell blocks script execution, run "Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope Process" first)*

On macOS / Linux:
python3 -m venv venv
source venv/bin/activate

### 2. Install Dependencies

pip install -r requirements.txt

This now also installs Celery and Redis's Python client, plus PyYAML for
the source registry.

### 3. Install and start Redis

Celery uses Redis as both the task broker and the crawl-state store.

Linux (Ubuntu/Debian):
sudo apt install redis-server
sudo systemctl start redis-server

macOS: `brew install redis && brew services start redis`
Windows: easiest is Redis via WSL, or a Docker container (`docker run -p 6379:6379 redis`).

Verify it's up: `redis-cli ping` should return `PONG`.

---

## Proxy Services Setup

### Tor Setup
Run a Tor daemon (or point at a controlled Tor gateway) exposing a SOCKS5 proxy.

* Windows (Tor Browser method): Open Tor Browser in the background. It exposes a SOCKS5 proxy on 127.0.0.1:9150. Update TOR_SOCKS_PROXY in Crawler/settings.py to match 9150.
* Windows (Standalone Daemon): Install via Chocolatey (choco install tor) or Scoop (scoop install tor) and run "tor" in PowerShell (default port 9050).
* Linux (Ubuntu/Debian):
  sudo apt install tor
  sudo systemctl start tor

### I2P Setup
Run an I2P router; its HTTP proxy defaults to 127.0.0.1:4444.

Both endpoints are configurable in Crawler/settings.py (TOR_SOCKS_PROXY, I2P_HTTP_PROXY) — point them at whatever controlled gateway your environment uses instead of a local daemon.

---

## Running a single crawl directly (unchanged)

# normal site
scrapy crawl single_source -a url="https://example.com" -a source_type="normal"

# onion site (routed through Tor automatically — no extra flag needed)
scrapy crawl single_source -a url="http://<address>.onion/" -a source_type="onion_site"

# i2p site
scrapy crawl single_source -a url="http://<address>.i2p/" -a source_type="blog_news"

-a source_name= is optional (defaults to source_1) — it's just the label that ends up in the metadata record.

---

## Running the queue-driven engine (multiple sources)

### 1. Define your sources

Edit `sources.yaml`. Each entry needs a name, URL, source_type, priority
(`fast` / `standard` / `bulk` — which Celery queue it's dispatched to), and
`interval_minutes` (how often it's due to be re-crawled). Set `enabled: false`
to keep a source registered but skip it.

### 2. Start a worker

celery -A engine.celery_app worker -Q fast,standard,bulk --concurrency=4 --loglevel=INFO

This one worker command listens on all three queues. In production you'd
typically run separate worker processes per queue so a flood of `bulk` jobs
can't starve `fast` ones — e.g. `-Q fast --concurrency=4` and `-Q bulk
--concurrency=2` as separate processes — but one worker on all three is
fine to start with.

### 3. Start the scheduler (Celery beat)

celery -A engine.celery_app beat --loglevel=INFO

This ticks every `SCHEDULER_TICK_SECONDS` (default 30s; set via env var),
checks `sources.yaml` against each source's crawl state in Redis, and
enqueues a job for anything that's due and not already running.

You can also trigger one scheduling pass manually without beat, e.g. for
testing:

python -c "from engine.scheduler import check_and_schedule; print(check_and_schedule.apply_async(queue='standard').get())"

### What you get out

Same as the single-spider output, now written to by every source:
- `crawl_output/raw/<sha256>.html` — preserved original content.
- `crawl_output/crawl_results.jsonl` — one record per fetch attempt, success
  or failure, across all sources.
- `crawl_output/dead_letter.jsonl` — jobs that failed repeatedly at the
  *process* level (crashed, timed out) after exhausting job-level retries.

Crawl state per source (last run time, success/failure counts, whether
it's currently running) lives in Redis under `crawlstate:<source_name>` —
inspect it with `redis-cli hgetall crawlstate:news_source_1`.

---

## How routing works

ProxyRoutingMiddleware (Crawler/middlewares.py) looks at the hostname of each request:
* .onion -> Tor SOCKS5 proxy
* .i2p -> I2P HTTP proxy
* Anything else -> Direct connection

Same spider, same code path, for all three.

---

## Reliability behavior — two layers

**Request-level** (inside one Scrapy run, `Crawler/middlewares.py`):
- `DOWNLOAD_TIMEOUT = 60` — generous, since Tor/I2P circuit setup is slow.
- Retryable failures (5xx, 408, 429, connection/DNS errors) retry with
  exponential backoff, capped at 3 attempts.
- A page that exhausts its retries gets a `status: failed` record written
  straight to `crawl_results.jsonl` — Scrapy's own retry-exhaustion path
  doesn't call the spider's errback (confirmed during testing, true of
  stock Scrapy too), so the middleware writes the failed result directly
  rather than depending on that.
- AUTOTHROTTLE + per-domain concurrency limit (1) keep this polite to a
  single source.

**Job-level** (across Scrapy runs, `engine/tasks.py`): this is a separate
layer for when the whole crawl process itself is the problem — it crashed,
hung past its timeout, or exited cleanly but the fetch inside it still
failed (checked by reading back the crawl-result record the run just
produced, not just the process exit code).
- Failed jobs retry with exponential backoff (10s, 20s, 40s), capped at 3
  attempts, separate from and on top of the request-level retries above.
- A job that exhausts its retries is dead-lettered: logged to
  `crawl_output/dead_letter.jsonl` and marked in the source's Redis state
  for operator review, instead of silently vanishing.

**Scheduling fairness**: the scheduler skips any source whose last job is
still `in_progress`, so one slow or stuck source can't pile up duplicate
jobs. `worker_prefetch_multiplier = 1` in the Celery config spreads work
evenly across sources rather than letting one worker slot hoard a batch
from a single fast-producing source.

---

## Not built yet (next layers)

- Link-following within scope/depth limits.
- Incremental crawling (ETag/Last-Modified, forum thread cursors).
- Requeueing from the dead-letter log (currently a log for manual operator
  review, not an automatic retry queue).
- Separate worker processes per queue tier for true fast/standard/bulk
  isolation (currently documented as a "next step," not set up by default).