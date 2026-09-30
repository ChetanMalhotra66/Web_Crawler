"""
Scheduler: the periodic tick that decides what to crawl next.

"The scheduler checks which sources are due for crawling and creates the
required tasks" (design doc). Runs as a Celery beat task on a fixed
interval (SCHEDULER_TICK_SECONDS, default 30s). For each enabled source:
  - skip if a job for it is already in progress (source fairness — a slow
    or stuck source can't pile up duplicate jobs and starve the others)
  - skip if it isn't due yet (based on interval_minutes and last_crawled_ts)
  - otherwise enqueue a crawl job on the queue matching its priority
"""
import logging

from engine.celery_app import app
from engine.sources import load_sources
from engine.state import CrawlState
from engine.tasks import crawl_source

logger = logging.getLogger(__name__)
state = CrawlState()


@app.task
def check_and_schedule():
    scheduled = []
    for source in load_sources():
        if not source.get("enabled", True):
            continue

        name = source["name"]

        if state.is_in_progress(name):
            logger.debug("Skipping '%s': already in progress", name)
            continue

        if not state.is_due(name, source["interval_minutes"]):
            continue

        crawl_source.apply_async(args=[source], queue=source["priority"])
        scheduled.append(name)
        logger.info("Scheduled crawl for '%s' on queue '%s'", name, source["priority"])

    return {"scheduled": scheduled}
