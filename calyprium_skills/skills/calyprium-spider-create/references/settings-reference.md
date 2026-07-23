# Settings reference

Everything a spider author might put in `custom_settings`. **Rule of thumb:** you set
*behavior* knobs; the backend injects *credentials/infrastructure* at run time. Don't
duplicate the injected ones.

## Injected by the backend — DO NOT set these in spider code

These arrive automatically when Forge schedules the run. Setting them yourself will
either be overwritten or break auth:

- Proxy/stealth creds: `CALYPRIUM_API_KEY`, `VEIL_API_KEY`, `VEIL_USER_ID`,
  `VEIL_GATEWAY_URL`, `SPECTRE_SERVICE_URL`, `MIMIC_SERVICE_URL`, `MIMIC_LOCAL_PROXY_URL`.
- Storage: `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` (= the API key),
  `AWS_ENDPOINT_URL`, `S3_BATCH_PATH`, `S3_BATCH_SIZE`, `SPIDER_USER_ID`,
  `SPIDER_NAME`, `SPIDER_RUN_NUMBER`.
- Runtime: `TWISTED_REACTOR` (asyncio — required for local httpcloak), `JOBDIR`
  (resume), `PRISM_URL`.
- The whole middleware/pipeline/extension wiring (`DOWNLOADER_MIDDLEWARES`,
  `ITEM_PIPELINES`, `EXTENSIONS`).

**Never set `USER_AGENT`** — Spectre owns it. Overriding desyncs the fingerprint.

## Author-set: fetch strategy (Mimic)

| Setting | Values / default | Purpose |
|---------|------------------|---------|
| `MIMIC_LOCAL_FETCH` | `True` / default off | Local-first in-process fetch with cookie replay. **Recommended on.** |
| `MIMIC_ALL_REQUESTS` | `True` / `False` | Force every request through a real browser. Only for JS-rendered data. |
| `MIMIC_WAIT_UNTIL` | `load` / `domcontentloaded` / `networkidle` | Browser wait condition (browser modes). |
| `MIMIC_WAIT_AFTER_LOAD` | ms, e.g. `2000` | Extra wait after load (capture pre-hydration state). |
| `MIMIC_STEALTH_LEVEL` | `basic` / `moderate` (default) / `maximum` | Browser stealth aggressiveness. Auto-escalates to `maximum` on blocks. |
| `MIMIC_BROWSER_ENGINE` | e.g. `camoufox` / `playwright` / `nodriver` | Pin a browser worker (rarely needed). |

Per-request override: `meta={"mimic": True}` forces one request to the browser
regardless of the global flags.

## Author-set: fingerprint (Spectre) — usually leave default

| Setting | Purpose |
|---------|---------|
| `MIMIC_USE_SPECTRE` | default `True`; apply device fingerprints. |
| `SPECTRE_STICKY_SESSION` | keep one fingerprint for the run (vs rotate). |
| `SPECTRE_ROTATE_PER_REQUEST` | fresh fingerprint every request. |
| `SPECTRE_DEVICE_TYPE` / `SPECTRE_BROWSER_FAMILY` / `SPECTRE_OS_FAMILY` | constrain the fingerprint pool. |

## Author-set: proxy shaping (Veil) — usually leave default

| Setting | Purpose |
|---------|---------|
| `VEIL_PROXY_TYPE` | `datacenter` / `residential` / `residential_rotating`. |
| `VEIL_PROVIDER` | e.g. `webshare_rotating`, `webshare_static`, `evomi_rotating`. |
| `VEIL_PROFILE` | named routing profile. |

Only touch these when a site needs residential IPs (most stealth-heavy sites do —
the backend usually already routes them appropriately).

## Author-set: standard Scrapy tuning

| Setting | Typical value | Notes |
|---------|--------------|-------|
| `CONCURRENT_REQUESTS` | `8` → `16` | Start low on fresh IPs, then raise. |
| `CONCURRENT_REQUESTS_PER_DOMAIN` | match above | Single-domain spiders. |
| `DOWNLOAD_DELAY` | `0` (local-first) / `1–2` (browser) | Router rate-caps local-first itself. |
| `AUTOTHROTTLE_ENABLED` | `False` (local-first) / `True` (browser) | + `AUTOTHROTTLE_START_DELAY` / `_MAX_DELAY` / `_TARGET_CONCURRENCY`. |
| `RETRY_TIMES` | `2` | |
| `RETRY_HTTP_CODES` | `[520, 521, 522, 523, 524]` | **Infra codes only. Never retry 403/429** — that's a block, not a transient error. |
| `ROBOTSTXT_OBEY` | `False` | You already sourced URLs from Prism; obeying robots re-fetches and can block the run. |
| `DOWNLOAD_TIMEOUT` | `30`–`60` | Higher for browser rendering. |
| `LOG_LEVEL` | `INFO` | `DEBUG` when diagnosing. |
| `handle_httpstatus_list` | `[403, 429, 503]` | (class attr, not settings) — see block responses. |

## Author-set: platform pipelines/extensions (opt-in)

| Setting | Purpose |
|---------|---------|
| `RECRAWL_TRACKING_ENABLED` | `True` records crawled URLs for freshness → enables `recrawl://` later. |
| `PRISM_CHECKPOINT_ENABLED` | `True` persists Prism pagination offset so a restarted run resumes where it stopped. |
| `TARGETS_DISCOVERY_ENABLED` / `TARGETS_COMPLETION_ENABLED` | multi-stage crawls: emit/consume derived target URLs (`TARGETS_URL_FIELDS`, `TARGETS_NESTED_FIELDS`, `TARGETS_SPIDER_SLUG`). |

## A good default `custom_settings` for a catalog spider

```python
custom_settings = {
    "CONCURRENT_REQUESTS": 8,
    "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
    "DOWNLOAD_DELAY": 0,
    "AUTOTHROTTLE_ENABLED": False,
    "RETRY_TIMES": 2,
    "RETRY_HTTP_CODES": [520, 521, 522, 523, 524],
    "ROBOTSTXT_OBEY": False,
    "DOWNLOAD_TIMEOUT": 30,
    "LOG_LEVEL": "INFO",
    "MIMIC_LOCAL_FETCH": True,
    "MIMIC_ALL_REQUESTS": False,
    "RECRAWL_TRACKING_ENABLED": True,
    "PRISM_CHECKPOINT_ENABLED": True,
}
```
