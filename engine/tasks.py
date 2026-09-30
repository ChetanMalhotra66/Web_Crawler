"""
Worker task: fetch one source.

Deliberately shells out to `scrapy crawl single_source ...` rather than
driving Scrapy's CrawlerProcess in-process — Scrapy's Twisted reactor can
only start once per process and doesn't play well with Celery's worker
model, so each job gets a clean subprocess. The existing single-source
spider (Crawler/spiders/single_source.py) is the unit of work; this layer
is purely about *when* and *how often* it runs, across many sources.

This is a second, separate layer of retry from Scrapy's own per-request
BackoffRetryMiddleware: that one retries individual HTTP requests inside a
crawl. This one retries the whole crawl job if the process itself fails
(crash, timeout, non-zero exit) — same exponential-backoff-then-dead-letter
pattern, at the job level.
"""
import json
import logging
import os
import subprocess
import time
from datetime import datetime

from engine.celery_app import app
from engine.state import CrawlState

logger = logging.getLogger(__name__)
state = CrawlState()

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEAD_LETTER_LOG = os.path.join(PROJECT_ROOT, "crawl_output", "dead_letter.jsonl")
RESULT_LOG = os.path.join(PROJECT_ROOT, "crawl_output", "crawl_results.jsonl")

MAX_JOB_RETRIES = 3
JOB_RETRY_BASE_DELAY = 10  # seconds; doubles each retry


@app.task(bind=True, max_retries=MAX_JOB_RETRIES, ignore_result=False)
def crawl_source(self, source: dict):
    name = source["name"]
    state.mark_started(name)
    job_start = time.time()

    try:
        cmd = [
            "scrapy", "crawl", "single_source",
            "-a", f"url={source['url']}",
            "-a", f"source_name={name}",
            "-a", f"source_type={source['source_type']}",
        ]
        result = subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=source.get("timeout_seconds", 120),
        )

        if result.returncode != 0:
            raise RuntimeError(
                f"scrapy crawl exited {result.returncode} for source '{name}': "
                f"{result.stderr[-1500:]}"
            )

        # A non-zero exit code only catches the process crashing outright.
        # Scrapy exits 0 even when every page it fetched failed (DNS error,
        # timeout, etc.) — confirmed during testing. So a 0 exit code alone
        # doesn't mean the *fetch* succeeded; check the actual crawl-result
        # record BackoffRetryMiddleware/the pipeline wrote for this run.
        outcome = _latest_result_since(name, job_start)
        if outcome is None:
            raise RuntimeError(
                f"scrapy crawl for '{name}' exited cleanly but produced no "
                f"crawl-result record — treating as a failed fetch"
            )
        if outcome.get("status") != "success":
            raise RuntimeError(
                f"fetch failed for '{name}': {outcome.get('error', 'unknown error')}"
            )

        state.mark_success(name)
        logger.info("Crawl succeeded for source '%s'", name)
        return {"source": name, "status": "success"}

    except Exception as exc:
        attempt = self.request.retries
        if attempt < MAX_JOB_RETRIES:
            delay = JOB_RETRY_BASE_DELAY * (2 ** attempt)
            state.mark_retry(name, str(exc))
            logger.warning(
                "Crawl job failed for '%s' (attempt %d/%d), retrying in %ds: %s",
                name, attempt + 1, MAX_JOB_RETRIES, delay, exc,
            )
            raise self.retry(exc=exc, countdown=delay)

        # Exhausted job-level retries -> dead-letter for operator review.
        state.mark_dead_letter(name, str(exc))
        _append_dead_letter(source, str(exc))
        logger.error("DEAD-LETTER: source '%s' exhausted %d job retries: %s",
                     name, MAX_JOB_RETRIES, exc)
        return {"source": name, "status": "dead_letter", "error": str(exc)}

    finally:
        state.mark_finished(name)


def _latest_result_since(source_name: str, since_ts: float) -> dict | None:
    """
    Most recent crawl_results.jsonl record for this source at/after since_ts.
    The log is shared across concurrently-running jobs for other sources, so
    this filters by source_name and a timestamp floor rather than just
    reading the last line.
    """
    if not os.path.exists(RESULT_LOG):
        return None

    latest, latest_ts = None, None
    with open(RESULT_LOG, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if record.get("source_name") != source_name:
                continue
            try:
                record_ts = datetime.fromisoformat(record["fetch_timestamp"]).timestamp()
            except (KeyError, ValueError, TypeError):
                continue
            if record_ts < since_ts - 2:  # small buffer for clock skew
                continue
            if latest_ts is None or record_ts > latest_ts:
                latest, latest_ts = record, record_ts
    return latest


def _append_dead_letter(source: dict, reason: str) -> None:
    os.makedirs(os.path.dirname(DEAD_LETTER_LOG), exist_ok=True)
    record = {
        "source_name": source["name"],
        "url": source["url"],
        "source_type": source["source_type"],
        "reason": reason[:1500],
        "timestamp": time.time(),
    }
    with open(DEAD_LETTER_LOG, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")
