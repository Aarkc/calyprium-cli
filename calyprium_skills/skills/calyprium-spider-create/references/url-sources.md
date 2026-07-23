# URL sources (`url_source`)

`PrismSitemapSpider` (and the DigiKey plain spiders) decide what to crawl from a
`url_source` string, passed at run time via `--arg url_source=...` or defaulted from
the class's `prism_domain`/`prism_path_prefix`. The scheme picks the strategy.

| Scheme | Example | What it does |
|--------|---------|--------------|
| `prism://` | `prism://www.example.com?path_prefix=/products/&pattern=...` | Pull pre-indexed URLs from Prism's URL database for that domain, lazily paginated. **The normal production source.** Requires Prism to have already analyzed the domain. |
| `recrawl://` | `recrawl://my_spider` | Pull only URLs that are *stale* (previously crawled, now due for refresh) from Forge for this spider slug. Use for scheduled freshness runs. Needs `RECRAWL_TRACKING_ENABLED=True` on prior runs. |
| `targets://` | `targets://my_spider?target_type=detail` | Pull pending derived targets — URLs discovered by another spider and queued for this one (multi-stage crawls: a listing spider feeds a detail spider). |
| `file://` | `file:///data/urls.txt` | One URL per line from a file on the Scrapyd host (newline-delimited, `#` comments allowed). Handy for ad-hoc lists. |
| `inline://` | `inline://https://a.com/1,https://a.com/2` | Comma-separated URLs inline. Good for a quick test run. |
| *(bare URL)* | `https://www.example.com/page` | Just crawl that single URL. Fastest way to smoke-test extraction. |

## Choosing one

- **First extraction test:** `--arg url_source=https://www.example.com/one-real-page`
  (bare URL) or `inline://...` — one page, fast feedback on your parser.
- **Small validation run:** `prism://<domain>` **plus `--arg max_urls=20`** — confirm
  the Prism URL set and stealth work end to end before scaling.
- **Full production run:** `prism://<domain>?path_prefix=...` with no `max_urls` cap.
- **Scheduled refresh:** `recrawl://<slug>` (after enabling recrawl tracking).
- **Two-stage crawl:** listing spider yields detail URLs (via the targets pipeline);
  detail spider runs `targets://<detail_slug>`.

## Prism prerequisite

`prism://` and the default source only work if Prism has indexed the domain. Check /
kick off analysis with the CLI's intel command:

```bash
calyprium intel www.example.com          # domain analysis: sitemaps, detection, strategy
```

If Prism has no URLs for the domain yet, either run intel/sitemap discovery first, or
seed the spider with `file://` / `inline://` / a bare URL while the index builds.

## The `max_urls` cap

`--arg max_urls=N` bounds total URLs crawled (0 = unlimited). **Always set a small cap
on the first real run** (e.g. 20–100) so a broken selector or a block wall doesn't
burn thousands of proxy requests before you notice. Raise it once `results --preview`
looks right.
