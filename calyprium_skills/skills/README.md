# Calyprium spider skills

A self-contained set of Claude Code agent skills for creating, deploying, and
debugging Calyprium web scrapers. They live here in the **`calyprium-cli`** repo
because their center of gravity is the `calyprium` CLI (the entry point and only hard
dependency of the spider workflow). The skills are self-contained — the `templates/`
are the canonical example spiders, so nothing needs to be checked out from the other
repos to use them. Cross-references between skills use skill-relative paths (e.g.
`calyprium-spider-create/references/...`), so keep the three folders as siblings if
you relocate them again.

## The skills

| Skill | Use it when |
|-------|-------------|
| **`calyprium-spider-create`** | Writing a new spider `.py` — base class choice, stealth strategy, extraction patterns, `custom_settings`. Has `references/` (deep docs) + `templates/` (copy-paste spiders). The main entry point; the other two link back to its references. |
| **`calyprium-spider-deploy`** | Operating the `calyprium` CLI — install, auth, `spider deploy/run/status/logs/results`, autonomous `scrape`. |
| **`calyprium-spider-debug`** | A spider is failing, blocked, or returning nothing — log reading, failure signatures, fixes. |

## Repos these skills reference

- **`calyprium-cli`** (this repo) — the `calyprium` CLI (single-file `calyprium.py`,
  PyPI package `calyprium`). Drives deploy/run/results against the Forge backend.
- **`scrapy-calyprium`** — the pip package (`scrapy_calyprium`) that spiders import:
  `PrismSitemapSpider`, the Veil/Mimic/Spectre middleware, `S3BatchPipeline`,
  local-first routing. Pre-installed in the Scrapyd image. This is the API the
  templates are built against.
- **`calyprium`** (the platform monorepo) — not needed to use the skills, but holds
  live "see also" ground truth: real spiders in `forge/spiders/`
  (`digikey.py`, `digikey_fast.py`, `digikey_light.py`), deploy/run logic in
  `forge/api/services/`, and design notes in `docs/design/`.

## Maintenance notes

- The `templates/` and `references/` in `calyprium-spider-create` are the authoritative
  guidance. The `calyprium` monorepo file paths cited in the references are "see also"
  pointers — they may drift, since they live in a different repo. Ground truth for the
  package API is the `scrapy-calyprium` repo; if it shifts, update the references and
  templates here.
- The `scrapy-calyprium` version actually deployed by Scrapyd is pinned in the
  `calyprium` monorepo's `forge/scraping/Dockerfile`
  (`scrapy-calyprium[local]==<version>`), not in the spider's `setup.py`.
