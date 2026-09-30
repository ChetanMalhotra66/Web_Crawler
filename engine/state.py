"""
Crawl state, per the design doc's "Crawl State" section: pending work,
completed work, retries, source health, and (later) incremental positions.

Backed by Redis so it's shared correctly across multiple Celery workers,
not a local file that would race under concurrent access.

One hash per source at key crawlstate:<source_name> with fields:
    last_crawled_ts      - unix timestamp of last completed attempt (any outcome)
    last_success_ts       - unix timestamp of last successful fetch
    last_status            - "success" | "failed" | "dead_letter" | "in_progress"
    consecutive_failures    - resets to 0 on success; used for source health
    total_success / total_failed - running counters
    in_progress             - "1" while a job for this source is executing
"""
import os
import time

import redis

REDIS_URL = os.environ.get("CRAWL_STATE_REDIS_URL", "redis://localhost:6379/2")


class CrawlState:
    def __init__(self, redis_url: str = REDIS_URL):
        self.r = redis.Redis.from_url(redis_url, decode_responses=True)

    def _key(self, source_name: str) -> str:
        return f"crawlstate:{source_name}"

    def is_in_progress(self, source_name: str) -> bool:
        return self.r.hget(self._key(source_name), "in_progress") == "1"

    def is_due(self, source_name: str, interval_minutes: int) -> bool:
        last = self.r.hget(self._key(source_name), "last_crawled_ts")
        if last is None:
            return True  # never crawled -> due immediately
        return (time.time() - float(last)) >= interval_minutes * 60

    def mark_started(self, source_name: str) -> None:
        self.r.hset(self._key(source_name), mapping={
            "in_progress": "1",
            "last_status": "in_progress",
        })

    def mark_success(self, source_name: str) -> None:
        now = time.time()
        self.r.hset(self._key(source_name), mapping={
            "last_crawled_ts": now,
            "last_success_ts": now,
            "last_status": "success",
            "consecutive_failures": 0,
        })
        self.r.hincrby(self._key(source_name), "total_success", 1)

    def mark_retry(self, source_name: str, reason: str) -> None:
        self.r.hset(self._key(source_name), mapping={
            "last_status": "retrying",
            "last_error": reason[:500],
        })

    def mark_dead_letter(self, source_name: str, reason: str) -> None:
        now = time.time()
        self.r.hset(self._key(source_name), mapping={
            "last_crawled_ts": now,
            "last_status": "dead_letter",
            "last_error": reason[:500],
        })
        self.r.hincrby(self._key(source_name), "total_failed", 1)
        self.r.hincrby(self._key(source_name), "consecutive_failures", 1)

    def mark_finished(self, source_name: str) -> None:
        self.r.hset(self._key(source_name), "in_progress", "0")

    def get_health(self, source_name: str) -> dict:
        return self.r.hgetall(self._key(source_name))

    def all_states(self, source_names: list[str]) -> dict:
        return {name: self.get_health(name) for name in source_names}
