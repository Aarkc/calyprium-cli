# Stealth and routing

How a Calyprium spider fetches pages without getting blocked. All of this is provided
by the `scrapy_calyprium` middleware stack, injected by Forge at deploy time. You
control it entirely through `custom_settings` and per-request `meta` — you never
instantiate proxies, browsers, or fingerprints yourself.

## The middleware stack (runs in this order)

| Order | Middleware | Role | Author action |
|-------|-----------|------|---------------|
| 100 | `VeilProxyMiddleware` | Routes every request through the Veil proxy gateway; sets `meta["proxy"]` + `Proxy-Authorization`. Credentials injected by the backend. | **None.** Always on. Never set proxy creds. If `VEIL_USER_ID`/API key are missing the spider is closed. |
| 150 | `SpectreMiddleware` | Applies a realistic device fingerprint (User-Agent + client-hint headers) per request/domain; rotates on block. | **None**, unless you want to pin/rotate — see settings. Do not override `USER_AGENT`. |
| 200 | `MimicBrowserMiddleware` | Fetches the body: local-first TLS-fingerprinted HTTP, or a real stealth browser for challenges/JS rendering. | Choose the strategy via settings + `meta` (below). |

The `S3BatchPipeline` (storage) and `CalypriumRunStats` / `CalypriumRequestTracer`
extensions are also injected — you don't configure them.

## Choose a fetch strategy

### 1. Local-first — the recommended default
```python
custom_settings = {
    "MIMIC_LOCAL_FETCH": True,     # in-process httpcloak/curl_cffi with cookie replay
    "MIMIC_ALL_REQUESTS": False,   # do NOT force every request through a browser
}
```
Flow inside the router for each request: try a light TLS-fingerprinted fetch (no
cookies) → if blocked and a cookie slot exists, replay with cached clearance cookies
+ matching TLS preset + pinned egress IP → if still blocked, call Mimic `/api/solve`
(real browser solves the Cloudflare/WAF challenge, returns fresh cookies) → cache the
new slot and retry. This is fast (~200ms warm) and only pays the ~real-browser cost
when a domain actually challenges you.

Use it whenever the data you need is present in the **initial HTTP response** —
static HTML, or server-rendered JSON like Next.js `__NEXT_DATA__`, `__NUXT__`,
`application/ld+json`, or an internal JSON API. This covers the large majority of
sites.

### 2. Browser-all — only when JS must run
```python
custom_settings = {
    "MIMIC_ALL_REQUESTS": True,
    "MIMIC_WAIT_UNTIL": "domcontentloaded",   # or "load" / "networkidle"
    "MIMIC_WAIT_AFTER_LOAD": 2000,            # ms to wait after load event
}
```
Every request is rendered in a real stealth browser. Slow and resource-heavy — use
only when the target data is injected by client-side JS after load and isn't in any
initial payload or API. Note: even Next.js `__NEXT_DATA__` is usually in the initial
HTML, so prefer local-first and only fall back to this if the blob is missing.
`domcontentloaded` + a short `MIMIC_WAIT_AFTER_LOAD` often captures `__NEXT_DATA__`
before React hydration strips it.

### 3. Per-request browser escalation — hybrid
Leave both global flags off (or local-first on) and mark only the requests that need
rendering:
```python
yield scrapy.Request(url, callback=self.parse_x, meta={"mimic": True})
```
`meta={"mimic": True}` (also `"playwright"` / `"stealth"`) forces that one request
through the browser. This is the escalation lever used in the block pattern below.

## Block detection + escalation pattern

Because you set `handle_httpstatus_list = [403, 429, 503]`, your parser receives block
responses. Handle them deliberately. The pattern below (from the `digikey_fast.py`
spider in the `calyprium` monorepo, and distilled into `templates/prism_spider.py`):

```python
def parse_item(self, response):
    # Hard block status -> give up on this URL. Do NOT retry via browser:
    # the local-first router already tried cookies + /api/solve. Retrying
    # burns another egress IP for the same result and accelerates pool
    # exhaustion. Let recrawl pick it up later.
    if response.status in (403, 429, 503):
        self._stats["failed"] += 1
        return

    data = self._extract_next_data(response)
    if data:
        item = self._parse_from_json(response.url, data)
        if item:
            yield item
            return

    # A 200 with no expected data = SILENT Cloudflare block (JS challenge
    # page returned 200). Tell the router so the slot's rate cap reacts and
    # the cookie pool rotates:
    self._signal_silent_failure(response, reason="no_data")

    # Small 200 body -> likely a JS challenge. Escalate THIS request to a
    # real browser once (guard on meta so you don't loop):
    if not response.meta.get("mimic") and len(response.text) < 20000:
        yield response.request.replace(
            meta={**response.meta, "mimic": True},
            dont_filter=True,
        )
        return

    # Browser response still without data -> DOM fallback, then give up.
    if response.meta.get("mimic"):
        item = self._parse_from_dom(response)
        if item:
            yield item
            return

    self._stats["failed"] += 1
```

### Signalling silent blocks back to the router
When local-first is on, the router is exposed as `self.mimic_router`, and the
middleware stuffs `meta["mimic_domain"]` and `meta["mimic_slot_id"]`. A 200 that
passed automated block detection but has no useful data is a *silent* block — report
it so the router can rate-cap and rotate the cookie slot:

```python
def _signal_silent_failure(self, response, reason: str) -> None:
    router = getattr(self, "mimic_router", None)
    domain = response.meta.get("mimic_domain")
    slot_id = response.meta.get("mimic_slot_id")
    if router is None or not domain:
        return
    router.report_silent_failure(domain, slot_id, reason=reason)
```

## Rate & concurrency guidance (avoiding blocks)

From the DigiKey stealth handoff — the hard-won defaults for Cloudflare-protected
sites behind rotating residential proxies:

- **Start concurrency low (8), validate, then raise to 16.** Too aggressive on fresh
  IPs trips detection and starts a pool-exhaustion death spiral.
- `DOWNLOAD_DELAY = 0` with `AUTOTHROTTLE_ENABLED = False` for local-first (the router
  does its own per-slot rate capping). For browser-all, *enable* autothrottle and use
  a small `CONCURRENT_REQUESTS` (~4).
- **Do not retry 403s.** Set `RETRY_HTTP_CODES` to infra codes only
  (`[520, 521, 522, 523, 524]`), not `403/429`. Retrying a block just wastes IPs.
- Fresh cookies often 403 their first 1–2 replays before Cloudflare accepts them —
  this is expected warmup, not a failure. Don't tear the slot down on the first miss.
- Let the **recrawl** mechanism (`RECRAWL_TRACKING_ENABLED`) pick up the URLs you
  missed rather than hammering them in-run.

## Sticky sessions (advanced)

For sites that bind clearance to an egress IP, you want the same residential IP for
the life of a cookie set. The router handles this via proxy `session_<id>` tokens
appended to the Veil username, and `MIMIC_LOCAL_PROXY_URL` (injected). The
`digikey_light.py` spider (in the `calyprium` monorepo, `forge/spiders/`) shows a
fully custom download handler + `SessionPool` that pins Evomi residential IPs
(~40 min) — only reach for that pattern when the standard local-first cookie slots
aren't holding. For most spiders, local-first's built-in slot pinning is enough.
