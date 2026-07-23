---
name: calyprium-spider-deploy
description: >
  Drive the Calyprium `calyprium` CLI to deploy, run, monitor, and collect results
  from a spider — the full lifecycle after the spider .py is written. Use when the
  user wants to deploy/upload/publish a spider, run/schedule/trigger a crawl, check
  spider status or logs, fetch scraped results, list spiders, authenticate the CLI,
  or generate a spider autonomously from a prompt (`scrape`). Covers install, auth
  (API key vs login), and every `calyprium spider` subcommand with exact flags.
---

# Deploying and running Calyprium spiders (CLI)

The `calyprium` CLI (repo: `C:\Users\chris\GitHub\calyprium-cli`, single-file
`calyprium.py`, PyPI package `calyprium`) talks to the Forge backend to deploy and
run spiders. To *author* the spider file itself, use the `calyprium-spider-create`
skill; this skill is the operational lifecycle.

## Install + authenticate (one time)

```bash
pip install calyprium               # command is `calyprium`; also `python -m calyprium`
```

Auth resolves in this order (first match wins):
1. **`CALYPRIUM_API_KEY` env var** (format `clp_...`) — **use this for automation/CI.**
   ```bash
   export CALYPRIUM_API_KEY=clp_xxxxxxxx
   ```
2. Tokens from an interactive browser login, stored at `~/.calyprium/tokens.json`
   (auto-refreshed):
   ```bash
   calyprium login          # opens Keycloak PKCE flow, callback on localhost:11899
   calyprium logout         # deletes stored tokens
   ```
3. Keycloak client-credentials grant if `KEYCLOAK_CLIENT_SECRET` is set (service accts).

All requests send `Authorization: Bearer <token>`. If a command 401s, re-auth.

### Configuration (env vars, defaults point at prod)
`CALYPRIUM_URL` (agent), `FORGE_URL` (spider backend, default
`https://forge.calyprium.com`), `MIMIC_URL`, `PRISM_URL`, plus the `KEYCLOAK_*` vars.
A `.env` beside the CLI is auto-loaded. You rarely change these — defaults are prod.

## The spider lifecycle

### Deploy — upload a spider file
```bash
calyprium spider deploy <file.py> [--name "Human Name"] [--slug custom_slug]
```
- Reads the file's source and POSTs `{name, code[, slug]}` to `POST /spiders`.
- `--name` defaults to the filename (title-cased). `--slug` is optional — **the
  server auto-generates one** (e.g. `digikey_cerulean`) if omitted.
- Redeploying the same slug creates a new version and redeploys.
- Prints `Deployed: <slug>` — note the slug, you need it for every other command.

### Run — schedule a crawl
```bash
calyprium spider run <slug> [--arg KEY=VALUE ...] [--setting KEY=VALUE ...]
```
- `--arg KEY=VALUE` (repeatable) → the spider's `__init__` kwargs (all strings).
  Common: `--arg max_urls=100`, `--arg url_source=prism://www.example.com`.
- `--setting KEY=VALUE` (repeatable) → Scrapy setting overrides for this run.
- Must be `KEY=VALUE` or it errors. Prints a `run_number` and `job_id`.
- **First run: always cap it** — `--arg max_urls=20` — then scale up.

### Monitor
```bash
calyprium spider list                          # all your spiders (slug/name/updated)
calyprium spider status <slug>                 # runs table: run#/status/items/started/duration
calyprium spider logs <slug> [--run N] [-n 200]  # log tail (default 100 lines, latest run)
```
Statuses: `running`, `finished`, `error`. When diagnosing failures/blocks, use the
`calyprium-spider-debug` skill.

### Collect results
```bash
calyprium spider results <slug> --preview                 # print first 5 items
calyprium spider results <slug> --preview --max-items 20  # print first 20
calyprium spider results <slug> -o out.jsonl              # save all (default {slug}_results.jsonl)
calyprium spider results <slug> --run N -o run_N.jsonl    # a specific run
```
Pulls the JSONL batches the `S3BatchPipeline` wrote to MinIO (via presigned
download), falling back to Scrapyd's item store. `--max-items 0` = everything.

## Autonomous generation (`scrape`)

Instead of hand-writing a spider, have the backend agent generate + deploy one from a
natural-language spec:
```bash
calyprium scrape <url> "<what data to extract>" [--max-items N] [--max-pages N] [--no-stream]
# e.g.
calyprium scrape https://books.toscrape.com "book titles and prices" --max-items 50
```
It streams a pipeline (Recon → API Discovery → Strategy → Selectors → Generate →
Validate → Iterate → Report), deploys the result, and returns a `spider_slug`. Then
use `spider results <slug> --preview` as usual. Resume/inspect a run with
`calyprium data <thread_id>`. Use this for a quick spider on a simple site; hand-write
(the `calyprium-spider-create` skill) for stealth-heavy or high-throughput targets.

## Other useful commands
- `calyprium fetch <url>` — one-off fetch through Mimic (probe a page during recon).
- `calyprium intel <domain>` — Prism domain analysis (sitemaps, bot detection,
  strategy). Run this before a `prism://` spider so the URL index exists.

## Notes / gotchas
- There is **no local scaffold/`new` command** and **no CLI scheduling command** —
  deploy uploads a single `.py`; recurring runs are orchestrated elsewhere (or via
  `recrawl://` sources + external cron).
- The slug is authoritative. If you lose it, `calyprium spider list`.
- `--setting` values override the backend-injected settings for that run only — handy
  for a one-off `--setting LOG_LEVEL=DEBUG` or `--setting CONCURRENT_REQUESTS=4`.
