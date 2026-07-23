---
name: calyprium-spider-create
description: >
  Author a new Calyprium web scraper (Scrapy spider) end to end — write the spider
  .py file, choose the right base class and stealth strategy, then deploy and run it
  with the `calyprium` CLI. Use whenever the user wants to create, write, scaffold,
  or add a new spider/scraper for a site in the Calyprium platform, or asks how
  spiders work here. Covers PrismSitemapSpider vs plain scrapy.Spider, Veil/Mimic/
  Spectre stealth, __NEXT_DATA__/JSON extraction, and the deploy→run→results loop.
---

# Creating a Calyprium spider

A Calyprium spider is a **single self-contained `.py` file** containing one Scrapy
spider class. You write the file, then upload it with the `calyprium` CLI, which
sends the source to the Forge backend. Forge wraps it in a Scrapy project, injects
all platform settings (proxy auth, fingerprints, S3 storage, telemetry, the asyncio
reactor), builds an egg, and deploys it to Scrapyd. **You never write `settings.py`,
`scrapy.cfg`, or a project scaffold — just the one spider file.**

## The golden rules

These are the invariants that keep a spider working on the platform. Break them and
the spider will fail at deploy or silently misbehave.

1. **One file, one spider class.** No project layout, no `settings.py`. Put all
   per-spider tuning in a `custom_settings` dict on the class.
2. **Never set proxy credentials or `USER_AGENT` in spider code.** Veil (proxy) and
   Spectre (fingerprint) are wired in and authenticated by the backend at run time.
   Overriding the UA breaks fingerprint consistency and gets you blocked.
3. **Yield plain `dict` items.** Storage to S3/MinIO is handled by the injected
   `S3BatchPipeline` (flushes every 100 items). Do not write files or open S3
   yourself unless you are scraping binaries (rare — see the digikey_files pattern).
4. **Import platform base classes from the `scrapy_calyprium` package**, e.g.
   `from scrapy_calyprium.spiders import PrismSitemapSpider`. This is the pip package
   (repo: `scrapy-calyprium`), pre-installed in the Scrapyd image. Do not import the
   `calyprium_ext.*` copy (a reference mirror that lives in the `calyprium` monorepo
   at `forge/scraping/`) — it isn't installed as a package and isn't what runs.
5. **The class `name` becomes the slug.** Forge rewrites `name = "..."` to the
   deployed slug, but keep it sane and lowercase (`digikey_fast`).
6. **Add a `Usage:` docstring** with the three CLI commands (deploy/run/results) so
   the next person knows how to drive it. This is a repo convention.

## Pick a base class

Decide this first — it determines where URLs come from and which method you override.

| Situation | Base class | You implement | URLs come from |
|-----------|-----------|---------------|----------------|
| Site already indexed in Prism (large catalogs, sitemaps) | `scrapy_calyprium.spiders.PrismSitemapSpider` | `parse_item(self, response)` | Prism URL DB, lazily paginated (memory-safe for millions of URLs) |
| A handful of seed URLs / custom crawl / follow links | `scrapy.Spider` | `start_requests` + `parse_*` | `start_urls`, or your own `start_requests` |

**Default to `PrismSitemapSpider`** for any real production scrape of a catalog site —
it handles URL sourcing, lazy pagination, checkpoint/resume, and recrawl for you.
Use plain `scrapy.Spider` only for small/exploratory scrapes or when URLs aren't in
Prism.

Copy-paste starting points live in `templates/`:
- `templates/prism_spider.py` — the standard production spider (start here).
- `templates/simple_spider.py` — minimal plain `scrapy.Spider`.

## Pick a fetch/stealth strategy

Every request goes through the Veil proxy automatically. The question is how the page
body is fetched and whether a browser is needed. Choose one:

| Strategy | `custom_settings` | When |
|----------|-------------------|------|
| **Local-first (recommended default)** | `MIMIC_LOCAL_FETCH=True`, `MIMIC_ALL_REQUESTS=False` | Static HTML or server-rendered JSON in the initial payload (incl. Next.js `__NEXT_DATA__`). Fast in-process TLS-fingerprinted fetch with cookie replay; only escalates to a real browser (`/api/solve`) when blocked. |
| **Browser-all** | `MIMIC_ALL_REQUESTS=True`, `MIMIC_WAIT_UNTIL`, `MIMIC_WAIT_AFTER_LOAD` | The data only exists after client-side JS runs. Slow (~real browser per page). |
| **Per-request browser** | (neither global flag) yield requests with `meta={"mimic": True}` | Mostly cheap pages, a few need rendering. Escalate individually. |

The full decision tree, the block-and-escalate pattern, and how to signal silent
Cloudflare blocks back to the router are in **`references/stealth-and-routing.md`**.

## The workflow, condensed

```bash
# 0. one-time: install + authenticate the CLI (see calyprium-spider-deploy skill)
pip install calyprium
export CALYPRIUM_API_KEY=clp_...        # or: calyprium login

# 1. write the spider file (use templates/, follow the golden rules)

# 2. deploy — uploads the file's source to Forge, which builds+deploys the egg
calyprium spider deploy path/to/my_spider.py --name "My Spider"
#   -> prints: Deployed: my_spider

# 3. run (args map to the spider __init__ kwargs)
calyprium spider run my_spider --arg max_urls=100

# 4. watch + collect
calyprium spider status my_spider
calyprium spider logs my_spider -n 200
calyprium spider results my_spider --preview
calyprium spider results my_spider -o out.jsonl
```

Re-deploying the same slug creates a new version and redeploys. The full CLI
reference (auth precedence, every flag, scheduling, autonomous `scrape`) is the
**`calyprium-spider-deploy`** skill.

## How to build a spider (process)

1. **Recon the target.** Fetch one page and inspect it. Prefer, in order:
   (a) a JSON blob in the HTML — `script#__NEXT_DATA__` (Next.js), `__NUXT__`,
   `application/ld+json`, or an inline state object; (b) an internal JSON API the
   page calls (check network); (c) CSS/XPath on the DOM as a last resort. JSON
   sources are stabler and richer than DOM scraping. You can use `calyprium fetch
   <url>` or `calyprium intel <domain>` to probe.
2. **Choose base class + stealth strategy** (tables above).
3. **Write the class** from a template: `name`, `allowed_domains`,
   `handle_httpstatus_list = [403, 429, 503]` (so you see blocks), `custom_settings`,
   and the parse method yielding dicts. See `references/spider-anatomy.md` for a
   field-by-field breakdown.
4. **Handle blocks explicitly** in the parse method — check `response.status` and
   the block/escalation pattern in `references/stealth-and-routing.md`.
5. **Deploy small, verify, scale.** Run with a tight `--arg max_urls=20` first,
   check `results --preview`, then raise limits and concurrency.

## Reference material (load as needed)

- `references/spider-anatomy.md` — every class attribute, `__init__` args, parse
  methods, item shape, the JSON-extraction pattern, worked from the real DigiKey spiders.
- `references/stealth-and-routing.md` — Veil/Mimic/Spectre stack, local-first vs
  browser, block detection + escalation, sticky sessions, silent-block signalling.
- `references/url-sources.md` — `url_source` schemes (`prism://`, `recrawl://`,
  `targets://`, `file://`, `inline://`, bare URL) and when to use each.
- `references/settings-reference.md` — every `custom_settings` / setting key that
  matters to a spider author, with defaults and what's injected vs. author-set.
- `templates/prism_spider.py`, `templates/simple_spider.py` — copy-paste starting points.
