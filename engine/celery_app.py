"""
Celery application: Redis-backed task queue with fast/standard/bulk lanes,
per the design doc. The scheduler tick runs as a periodic (beat) task; actual
source fetches run as worker tasks routed to the queue matching the source's
configured priority.
"""
import os

from celery import Celery
from kombu import Exchange, Queue

BROKER_URL = os.environ.get("CELERY_BROKER_URL", "redis://localhost:6379/0")
RESULT_BACKEND = os.environ.get("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")

app = Celery(
    "darktrace_engine",
    broker=BROKER_URL,
    backend=RESULT_BACKEND,
    include=["engine.tasks", "engine.scheduler"],
)

app.conf.task_queues = (
    Queue("fast", Exchange("fast"), routing_key="fast"),
    Queue("standard", Exchange("standard"), routing_key="standard"),
    Queue("bulk", Exchange("bulk"), routing_key="bulk"),
)
# Jobs are routed explicitly per-call (queue=source["priority"]) since routing
# depends on which source the job is for, not the task name. This default
# only matters for tasks dispatched without an explicit queue.
app.conf.task_default_queue = "standard"
app.conf.task_default_exchange = "standard"
app.conf.task_default_routing_key = "standard"

app.conf.task_acks_late = True          # don't ack until the job actually finishes
app.conf.worker_prefetch_multiplier = 1  # one job at a time per worker slot -> fairer across sources
app.conf.task_track_started = True

# Scheduler tick: check which sources are due and enqueue jobs for them.
app.conf.beat_schedule = {
    "check-and-schedule-sources": {
        "task": "engine.scheduler.check_and_schedule",
        "schedule": float(os.environ.get("SCHEDULER_TICK_SECONDS", 30)),
    },
}
app.conf.timezone = "UTC"
