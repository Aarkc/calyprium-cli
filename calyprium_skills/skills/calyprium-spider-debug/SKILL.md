---
name: calyprium-spider-debug
description: >
  Diagnose and fix a Calyprium spider that is failing, getting blocked, deploying
  with errors, or returning empty/no results. Use when a spider run errors, yields
  zero or too few items, gets 403/429/503 blocks, times out, exhausts proxy IPs, or
  the extraction is wrong. Covers reading logs via the CLI, common failure signatures
  and their fixes, block/stealth escalation, and the deploy-time errors.
---

# Debugging a Calyprium spider

Work top-down: read the logs, match the signature below, apply the fix, redeploy,
re-run small. Assumes the spider is written (`calyprium-spider-create`) and the CLI
is set up (`calyprium-spider-deploy`).

## Step 1 — get the evidence

```bash
calyprium spider status <slug>                  # did it finish/error? how many items?
calyprium spider logs <slug> -n 300             # latest run's log tail
calyprium spider logs <slug> --run N -n 300     # a specific run
calyprium spider results <slug> --preview       # what actually came out
```
Re-run with more logging when needed: `calyprium spider run <slug> --arg max_urls=20
--setting LOG_LEVEL=DEBUG`. **Always debug with a small `max_urls`** so you don't burn
proxy IPs reproducing the problem.

## Step 2 — match the signature

### Deploy fails
- **`scrapyd-deploy` / egg build error, or slug never appears** — usually a Python
  syntax/import error in the spider file, or an import the Scrapyd image doesn't have.
  Only import from the standard lib, `scrapy`, and `scrapy_calyprium.*` (plus
  `boto3`/`requests`, which are present). Don't import `calyprium_ext.*` (a reference
  mirror in the `calyprium` monorepo, not installed as a package). Lint the file
  locally first:
  `python -c "import ast; ast.parse(open('my_spider.py').read())"`.
- **Spider not found after deploy** — the class must subclass `scrapy.Spider` (or
  `PrismSitemapSpider`) and define `name`. Check the class is at module top level.

### Spider closes immediately
- **Closed with a Veil/credentials error** — `VeilProxyMiddleware` raises `CloseSpider`
  if `VEIL_USER_ID` / API key aren't present. This is an environment/auth problem, not
  your code — verify the run was scheduled through Forge (it injects these) and the
  API key is valid.
- **`SPECTRE_USER_ID` missing** — same class of issue if Spectre is enabled.

### Zero or too few items
- **All requests 403/429/503** (see them in logs) — you're being blocked. Go to
  "Blocks" below.
- **200 responses but no items** — extraction is wrong OR it's a *silent* block (a
  challenge page returned 200). Check `results --preview` and log the body size. If
  bodies are small (<20KB) and lack your data marker, it's a silent block — enable the
  browser-escalation pattern (`calyprium-spider-create` → stealth-and-routing.md). If
  bodies are full pages, your selectors/JSON path are wrong — re-inspect one page with
  `calyprium fetch <url>` and fix `_extract_json` / `_parse_from_*`.
- **`No __NEXT_DATA__` / KeyError-swallowed empties** — the JSON path changed or the
  page renders client-side. Verify the marker exists in the raw HTML; if it only
  appears after JS, switch to `MIMIC_ALL_REQUESTS=True` with `MIMIC_WAIT_UNTIL=
  "domcontentloaded"` + `MIMIC_WAIT_AFTER_LOAD=2000`.
- **Empty because no URLs** — `prism://` needs Prism to have indexed the domain. Run
  `calyprium intel <domain>` first, or seed with `file://`/`inline://`/a bare URL.

### Blocks (403/429/503 or silent 200)
Fixes, in order of preference:
1. **Lower concurrency.** Drop `CONCURRENT_REQUESTS` to `8` (or `4` for browser mode)
   on fresh IPs; raise only after a clean run. Aggressive concurrency triggers
   detection and a pool-exhaustion death spiral.
2. **Stop retrying blocks.** Ensure `RETRY_HTTP_CODES` is infra-only
   (`[520,521,522,523,524]`) — retrying 403/429 wastes IPs. Don't re-yield blocked
   URLs to the browser either; let recrawl catch them.
3. **Confirm local-first + escalation is wired** — `MIMIC_LOCAL_FETCH=True`, and the
   parse method escalates small-body 200s to `meta={"mimic": True}` once (guarded).
4. **Tolerate cookie warmup.** Fresh cookies often 403 their first 1–2 replays before
   Cloudflare accepts them — that's expected, not a bug. Don't tear slots down on the
   first miss.
5. **Signal silent failures** so the router rate-caps and rotates
   (`router.report_silent_failure(...)` via `self.mimic_router`).
6. **Escalate stealth/residential** — for stubborn sites, `MIMIC_STEALTH_LEVEL=
   "maximum"`, residential proxy type, or the sticky-session pattern from
   `digikey_light.py`.

### Timeouts / OOM / slow
- **Timeouts** — raise `DOWNLOAD_TIMEOUT` (60 for browser rendering); check the proxy
  is reachable.
- **Memory blowup on huge catalogs** — use `PrismSitemapSpider` (lazy pagination) and
  a modest `batch_size` (5000). Don't materialize all URLs. Don't stuff giant `_raw_*`
  blobs if you don't need them.
- **Slow** — you're probably in browser-all mode unnecessarily. Move to
  `MIMIC_LOCAL_FETCH=True` if the data is in the initial payload.

## Step 3 — fix, redeploy, verify
```bash
calyprium spider deploy my_spider.py --name "My Spider"   # new version
calyprium spider run <slug> --arg max_urls=20             # small
calyprium spider results <slug> --preview                 # confirm before scaling
```

## Where the deep detail lives
- Stealth/blocks/escalation internals, rate guidance, sticky sessions →
  `calyprium-spider-create/references/stealth-and-routing.md`.
- Every setting and its default → `calyprium-spider-create/references/settings-reference.md`.
- Design context → `docs/design/digikey-stealth-fetch-handoff.md` in the `calyprium`
  repo (proxy chain, concurrency, no-403-retry rationale).
