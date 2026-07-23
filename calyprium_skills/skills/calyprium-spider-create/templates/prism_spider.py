"""
<Human Name> Spider

Scrapes <what> from <site>. Data lives in <where — e.g. the __NEXT_DATA__ JSON
blob / an inline state object / the DOM>. URLs come from Prism's pre-indexed
sitemap database (no re-crawling of sitemaps).

Local-first fetch: in-process TLS-fingerprinted HTTP with cached cookie replay,
escalating to a real browser only when Cloudflare/WAF challenges. Extends
PrismSitemapSpider for lazy, memory-bounded URL pagination.

Usage:
    calyprium spider deploy path/to/this_spider.py --name "<Human Name>"
    calyprium spider run <slug> --arg max_urls=20        # small validation run first
    calyprium spider results <slug> --preview
    calyprium spider run <slug>                          # full run once validated
"""
import json
import logging
import re
from typing import Optional

from scrapy_calyprium.spiders import PrismSitemapSpider

logger = logging.getLogger(__name__)


class MySpider(PrismSitemapSpider):
    name = "my_spider"                      # becomes the deploy slug
    allowed_domains = ["www.example.com"]

    # ── Prism URL sourcing ──
    prism_domain = "www.example.com"
    prism_path_prefix = "/products/detail/"  # restrict to detail pages; None = all
    batch_size = 5000                        # URLs held in memory at once

    # See block responses instead of Scrapy dropping them
    handle_httpstatus_list = [403, 429, 503]

    custom_settings = {
        "CONCURRENT_REQUESTS": 8,            # start low on fresh IPs, raise to 16
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "DOWNLOAD_DELAY": 0,
        "AUTOTHROTTLE_ENABLED": False,
        "RETRY_TIMES": 2,
        "RETRY_HTTP_CODES": [520, 521, 522, 523, 524],   # infra only, never 403/429
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_TIMEOUT": 30,
        "LOG_LEVEL": "INFO",
        # Local-first fetch (recommended default)
        "MIMIC_LOCAL_FETCH": True,
        "MIMIC_ALL_REQUESTS": False,
        # Platform features
        "RECRAWL_TRACKING_ENABLED": True,    # enables recrawl:// freshness runs later
        "PRISM_CHECKPOINT_ENABLED": True,    # resume pagination after a restart
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)    # base handles url_source/max_urls/batch_size
        self._stats = {"ok": 0, "blocked": 0, "browser": 0, "failed": 0}

    def closed(self, reason):
        logger.info("%s stats: %s", self.name, self._stats)

    # ── Parsing ───────────────────────────────────────────────────

    def parse_item(self, response):
        # 1. hard block -> give up on this URL (recrawl picks it up later).
        #    Do NOT retry via browser: the router already tried cookies + solve.
        if response.status in (403, 429, 503):
            self._stats["blocked"] += 1
            return

        # 2. primary extraction: structured JSON embedded in the page
        data = self._extract_json(response)
        if data:
            item = self._parse_from_json(response.url, data)
            if item:
                self._stats["ok"] += 1
                yield item
                return

        # 3. 200 without expected data = silent block. Tell the router so the
        #    slot rate-caps and the cookie pool rotates.
        self._signal_silent_failure(response, reason="no_data")

        # 4. small body -> likely a JS challenge; escalate this one request to a
        #    real browser (guard on meta so it can't loop).
        if not response.meta.get("mimic") and len(response.text) < 20000:
            self._stats["browser"] += 1
            yield response.request.replace(
                meta={**response.meta, "mimic": True},
                dont_filter=True,
            )
            return

        # 5. browser response still without data -> DOM fallback, then give up.
        if response.meta.get("mimic"):
            item = self._parse_from_dom(response)
            if item:
                self._stats["browser"] += 1
                yield item
                return

        self._stats["failed"] += 1
        logger.warning("no data extracted from %s", response.url)

    # ── Extraction helpers ────────────────────────────────────────

    def _extract_json(self, response) -> Optional[dict]:
        """Pull embedded JSON. Adjust the marker for your target site."""
        script = response.css("script#__NEXT_DATA__::text").get()
        if script:
            try:
                return json.loads(script)
            except json.JSONDecodeError:
                pass
        m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                      response.text, re.S)
        if m:
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                pass
        return None

    def _parse_from_json(self, url: str, data: dict) -> Optional[dict]:
        """Walk the JSON defensively — .get() everywhere, default None/[]/{}."""
        try:
            envelope = data["props"]["pageProps"]["envelope"]["data"]
        except (KeyError, TypeError):
            return None

        item = {"url": url, "source": self.name}
        overview = envelope.get("productOverview", {})
        item["title"] = overview.get("title")
        item["manufacturer_part_number"] = overview.get("manufacturerProductNumber")
        # ... map the fields you need ...

        item["_raw"] = json.dumps(envelope)   # keep raw blob so nothing is lost
        return item

    def _parse_from_dom(self, response) -> Optional[dict]:
        """CSS/XPath fallback for browser-rendered pages without the JSON blob."""
        item = {"url": response.url, "source": self.name}
        item["title"] = (response.css("h1::text").get("") or "").strip() or None
        # ... selectors ...
        return item if item.get("title") else None

    def _signal_silent_failure(self, response, reason: str) -> None:
        router = getattr(self, "mimic_router", None)
        domain = response.meta.get("mimic_domain")
        slot_id = response.meta.get("mimic_slot_id")
        if router is None or not domain:
            return
        router.report_silent_failure(domain, slot_id, reason=reason)
