"""
Request handling middlewares.

ProxyRoutingMiddleware  - routes each request through the right connectivity
                           (Tor SOCKS5 for .onion, I2P HTTP proxy for .i2p,
                           direct for everything else) based on the target
                           host, so the same spider can hit onion sites and
                           normal sites without per-request config.

BackoffRetryMiddleware  - retryable failures get retried with exponential
                           backoff, up to a max of 3 attempts, per the design
                           doc. Fatal / exhausted-retry requests are logged
                           as dead-lettered for operator review instead of
                           silently dropped, and a "failed" crawl-result
                           record is written so the failure shows up in the
                           same result log as successful fetches.
"""
import json
import logging
import os
import random
from datetime import datetime, timezone

from scrapy.downloadermiddlewares.retry import RetryMiddleware
from scrapy.utils.response import response_status_message
from twisted.internet import defer, reactor

logger = logging.getLogger(__name__)


class ProxyRoutingMiddleware:
    """Sets request.meta['proxy'] based on the target domain's TLD."""

    def __init__(self, tor_proxy, i2p_proxy):
        self.tor_proxy = tor_proxy
        self.i2p_proxy = i2p_proxy

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            tor_proxy=crawler.settings.get("TOR_SOCKS_PROXY"),
            i2p_proxy=crawler.settings.get("I2P_HTTP_PROXY"),
        )

    def process_request(self, request, spider):
        host = _host_of(request.url)

        if host.endswith(".onion"):
            request.meta["proxy"] = self.tor_proxy
            request.meta["network"] = "tor"
        elif host.endswith(".i2p"):
            request.meta["proxy"] = self.i2p_proxy
            request.meta["network"] = "i2p"
        else:
            # Normal site: no proxy, direct connection.
            request.meta["network"] = "clearnet"
        return None


def _host_of(url: str) -> str:
    # Local import to keep this file dependency-light.
    from urllib.parse import urlparse
    return (urlparse(url).hostname or "").lower()


class BackoffRetryMiddleware(RetryMiddleware):
    """RetryMiddleware with exponential backoff, capped at RETRY_TIMES (3)."""

    def __init__(self, settings):
        super().__init__(settings)
        self.base_delay = settings.getfloat("RETRY_BASE_DELAY", 2.0)

    def _retry(self, request, reason):
        # Scrapy >=2.13 dropped the `spider` arg here; get it from the crawler
        # instead (needed for stats access and the dead-letter path below).
        spider = self.crawler.spider
        retries = request.meta.get("retry_times", 0) + 1
        max_retry_times = request.meta.get("max_retry_times", self.max_retry_times)

        if retries <= max_retry_times:
            delay = self.base_delay * (2 ** (retries - 1))
            delay += random.uniform(0, 1)  # jitter
            logger.info(
                "Retrying %s (attempt %d/%d) after %.1fs — reason: %s",
                request.url, retries, max_retry_times, delay, reason,
            )
            d = defer.Deferred()
            reactor.callLater(delay, d.callback, None)

            def _do_retry(_):
                new_request = request.copy()
                new_request.meta["retry_times"] = retries
                new_request.dont_filter = True
                new_request.priority = request.priority + self.priority_adjust
                return new_request

            return d.addCallback(_do_retry)

        # Exhausted retries: dead-letter for operator review, per design doc.
        logger.warning(
            "DEAD-LETTER: %s exhausted %d retries — reason: %s",
            request.url, max_retry_times, reason,
        )
        stats = getattr(spider.crawler, "stats", None)
        if stats:
            stats.inc_value("darktrace/dead_letter_count")

        # Scrapy's own give-up path swallows the failure here rather than
        # propagating it to the request's errback (confirmed: this is true
        # even of stock RetryMiddleware, not something specific to this
        # subclass), so the spider's "failed" crawl-result item would never
        # get produced. Write the failed-result record directly to the same
        # log the success-path pipeline uses, rather than depending on
        # engine/pipeline machinery this middleware sits outside of.
        _write_failed_crawl_result(spider, request, reason, attempts=retries)
        return None


def _write_failed_crawl_result(spider, request, reason, attempts):
    settings = spider.crawler.settings
    log_path = settings.get("RESULT_LOG_PATH", "crawl_output/crawl_results.jsonl")
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)

    record = {
        "source_name": getattr(spider, "source_name", None),
        "source_type": getattr(spider, "source_type", None),
        "url": request.url,
        "network": request.meta.get("network", "clearnet"),
        "fetch_timestamp": datetime.now(timezone.utc).isoformat(),
        "crawler_build": settings.get("CRAWLER_BUILD"),
        "status": "failed",
        "http_status": None,
        "attempt": attempts,
        "title": None,
        "content_hash": None,
        "duplicate_of_existing": False,
        "evidence_path": None,
        "error": str(reason)[:500],
    }
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")