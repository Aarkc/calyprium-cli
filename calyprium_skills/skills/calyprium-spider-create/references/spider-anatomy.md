# Spider anatomy

A field-by-field breakdown of a Calyprium spider file. The `templates/` in this skill
are the authoritative examples. For live "see also" reference, the real production
spiders live in the `calyprium` monorepo at `forge/spiders/` (`digikey.py`,
`digikey_fast.py`, `digikey_light.py`, `digikey_files.py`).

## File skeleton

```python
"""
<Human name> Spider

<one-paragraph description: what it scrapes, where data lives, rendering notes>

Usage:
    calyprium spider deploy path/to/my_spider.py --name "My Spider"
    calyprium spider run my_spider --arg max_urls=100
    calyprium spider results my_spider --preview
"""
import json
import logging
from typing import Optional

from scrapy_calyprium.spiders import PrismSitemapSpider   # or: import scrapy

logger = logging.getLogger(__name__)


class MySpider(PrismSitemapSpider):
    name = "my_spider"                       # becomes the deploy slug
    allowed_domains = ["www.example.com"]

    # PrismSitemapSpider-only config:
    prism_domain = "www.example.com"         # required; which domain's URLs to pull
    prism_path_prefix = "/products/detail/"  # optional filter on the Prism URL set
    prism_pattern = None                     # optional regex/glob filter
    batch_size = 5000                        # URLs held in memory per page

    # Let the spider SEE block responses instead of Scrapy dropping them:
    handle_httpstatus_list = [403, 429, 503]

    custom_settings = {
        # ... per-spider tuning, see settings-reference.md ...
    }

    def parse_item(self, response):          # PrismSitemapSpider entry point
        ...
        yield item                           # a plain dict
```

## Class attributes

- `name` (**required**) — lowercase identifier. Forge rewrites this to the deploy
  slug, but set it to match.
- `allowed_domains` — standard Scrapy off-site filter. Set it.
- `handle_httpstatus_list = [403, 429, 503]` — **important.** Without this, Scrapy's
  HttpError middleware drops block responses before your parser runs, so you can't
  detect or react to blocks. Always include it.
- `custom_settings` (dict) — the only place you configure behavior. See
  `settings-reference.md`.

### PrismSitemapSpider-only attributes
- `prism_domain` (**required for Prism spiders**) — the domain whose pre-indexed URL
  set to crawl. Must match a domain Prism has analyzed.
- `prism_path_prefix` — restrict to URLs under a path (e.g. `/en/products/detail/`).
- `prism_pattern` — further filter (see the package docstring for exact semantics).
- `batch_size` — how many URLs are fetched from Prism per page of pagination.
  Smaller = less memory. 5000 is a good default for huge catalogs.

## `__init__` and CLI args

CLI `--arg KEY=VALUE` flags become `__init__` keyword arguments (all as **strings**).
For `PrismSitemapSpider` the base class already accepts `url_source`, `prism_url`,
`batch_size`, `max_urls` — you usually don't need a custom `__init__` at all.

If you add one, always accept `*args, **kwargs` and call `super().__init__()`, and
cast numeric args yourself:

```python
def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)        # base handles url_source/max_urls/etc.
    self._stats = {"ok": 0, "blocked": 0, "failed": 0}

def closed(self, reason):
    logger.info("stats: %s", self._stats)
```

Common base-class args you pass at run time (all optional):
- `--arg max_urls=100` — cap total URLs (0 = unlimited). **Always use a small cap on
  the first run.**
- `--arg url_source=...` — override where URLs come from (see `url-sources.md`).
- `--arg batch_size=5000`.

## The parse method

- `PrismSitemapSpider` → override **`parse_item(self, response)`**. The base class
  fetches Prism URLs and calls this for each page.
- plain `scrapy.Spider` → write `start_requests()` yielding `scrapy.Request(url,
  callback=self.parse_x)` and the callbacks.

Structure every parser as: **(1) block check → (2) primary JSON extraction → (3)
fallback → (4) yield dict / signal failure.**

```python
def parse_item(self, response):
    # 1. block check — you enabled handle_httpstatus_list, so react here
    if response.status in (403, 429, 503):
        self._stats["blocked"] += 1
        return

    # 2. primary: structured JSON embedded in the page
    data = self._extract_next_data(response)
    if data:
        item = self._parse_from_json(response.url, data)
        if item:
            self._stats["ok"] += 1
            yield item
            return

    # 3. fallback: DOM, or escalate to a browser (see stealth-and-routing.md)
    item = self._parse_from_dom(response)
    if item:
        yield item
        return

    self._stats["failed"] += 1
    logger.warning("no data extracted from %s", response.url)
```

## Extracting embedded JSON (preferred over DOM)

Most modern sites ship their data as JSON in the HTML. This is stabler and richer
than CSS selectors. The canonical pattern (from the DigiKey spiders) for Next.js:

```python
import json, re

def _extract_next_data(self, response) -> Optional[dict]:
    # fast path: the script tag by id
    script = response.css("script#__NEXT_DATA__::text").get()
    if script:
        try:
            return json.loads(script)
        except json.JSONDecodeError:
            pass
    # regex fallback (handles attribute-order / whitespace variance)
    m = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
                  response.text, re.S)
    if m:
        try:
            return json.loads(m.group(1))
        except json.JSONDecodeError:
            pass
    return None
```

Then walk the object defensively (never assume keys exist):

```python
def _parse_from_json(self, url, data):
    try:
        envelope = data["props"]["pageProps"]["envelope"]["data"]
    except (KeyError, TypeError):
        return None
    item = {"url": url, "source": "example"}
    item["title"] = envelope.get("productOverview", {}).get("title")
    # ... .get() chains everywhere; default to None/[]/{} ...
    item["_raw"] = json.dumps(envelope)      # keep the raw blob so nothing is lost
    return item
```

Other embedded-JSON markers to look for: `__NUXT__` (Nuxt), `window.__INITIAL_STATE__`
/ Redux-style state, `<script type="application/ld+json">` (schema.org), or an inline
`<script>` assigning a config object. Prism's `intel` output flags which apply.

## Item shape

Items are plain dicts. Conventions:
- Always include `url` and a `source` field (the site name).
- Keep a `_raw_*` field with the untransformed JSON/HTML so re-parsing later needs no
  re-crawl. (Only skip this if items are enormous and storage matters.)
- Use `None` / `[]` / `{}` for missing values, not absent keys — keeps the JSONL
  schema stable across rows.
- Nested structures (lists of pricing tiers, spec dicts) are fine; they serialize to
  JSONL.

## Two real reference spiders (see also — in the `calyprium` monorepo)

The `templates/` here mirror these patterns, so you don't need the monorepo. But if
you have it checked out, these are the ground-truth originals:

- **`forge/spiders/digikey_fast.py`** — `PrismSitemapSpider`, local-first
  (`MIMIC_LOCAL_FETCH=True`), `__NEXT_DATA__` extraction, browser escalation on soft
  block, silent-block signalling. **The best model for a high-throughput catalog
  scrape** — `templates/prism_spider.py` is distilled from it.
- **`forge/spiders/digikey.py`** — plain `scrapy.Spider` with its own Prism URL
  sourcing and `MIMIC_ALL_REQUESTS=True` browser rendering. Good for seeing the
  browser-all strategy and manual `start_requests`.
