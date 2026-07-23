"""
<Human Name> Spider

Minimal plain scrapy.Spider for a small or exploratory scrape where URLs are NOT
in Prism. Sources URLs from start_urls / a custom start_requests. Uses local-first
fetch; add meta={"mimic": True} to any request that needs a real browser.

Usage:
    calyprium spider deploy path/to/this_spider.py --name "<Human Name>"
    calyprium spider run <slug>
    calyprium spider results <slug> --preview
"""
import logging
from typing import Optional

import scrapy

logger = logging.getLogger(__name__)


class MySimpleSpider(scrapy.Spider):
    name = "my_simple_spider"               # becomes the deploy slug
    allowed_domains = ["www.example.com"]

    start_urls = [
        "https://www.example.com/page-1",
        "https://www.example.com/page-2",
    ]

    handle_httpstatus_list = [403, 429, 503]

    custom_settings = {
        "CONCURRENT_REQUESTS": 8,
        "DOWNLOAD_DELAY": 0,
        "AUTOTHROTTLE_ENABLED": False,
        "RETRY_TIMES": 2,
        "RETRY_HTTP_CODES": [520, 521, 522, 523, 524],
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_TIMEOUT": 30,
        "LOG_LEVEL": "INFO",
        "MIMIC_LOCAL_FETCH": True,
        "MIMIC_ALL_REQUESTS": False,
    }

    # Optional: accept CLI --arg overrides. Args arrive as strings.
    def __init__(self, start_url: Optional[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if start_url:
            self.start_urls = [start_url]

    def parse(self, response):
        if response.status in (403, 429, 503):
            logger.warning("blocked (%s): %s", response.status, response.url)
            return

        # Extract fields (prefer embedded JSON; fall back to selectors)
        yield {
            "url": response.url,
            "source": self.name,
            "title": (response.css("h1::text").get("") or "").strip() or None,
            # ... more fields ...
        }

        # Follow links (example):
        # for href in response.css("a.next::attr(href)").getall():
        #     yield response.follow(href, callback=self.parse)
