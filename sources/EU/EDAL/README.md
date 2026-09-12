# EU/EDAL - European Database of Asylum Law

Asylum case law summaries maintained by ECRE (European Council on Refugees and Exiles),
covering the CJEU, the ECtHR, UN treaty bodies and the national courts of 20+ EU member
states.

## Data Source

- **URL**: https://www.asylumlawdatabase.eu/summaries
- **Method**: sitemap-driven HTML scraping (server-rendered Laravel pages)
- **Authentication**: None
- **Coverage**: ~1,830 cases (CJEU 136, ECtHR 266, national 1,427, UN 3), updated through 2026
- **Crawl delay**: 1 second (robots.txt allows `User-agent: *` outside /admin, /user, /core and no longer declares a Crawl-delay)
- **Content**: Structured English summaries — headnote, facts, decision & reasoning, outcome, subsequent proceedings, observations

## Access path

Discovery goes through the sitemap index rather than the listing page, which is a
Livewire (JS) table:

```
/sitemap.xml                          sitemap index
/sitemap.xml/summaries/cjeu           CJEU case URLs
/sitemap.xml/summaries/ecrthr         ECtHR case URLs
/sitemap.xml/summaries/national       national court case URLs
/sitemap.xml/summaries/un             UN treaty body case URLs
/summaries/case/{slug}                case page (server-rendered HTML)
```

The four sitemaps are round-robin interleaved and each is walked newest-first, so any
prefix of a run (including `--sample`) spans all four case-law streams and starts with
the most recent decisions.

On a case page the analytical body lives in `<section data-section-id="...">` blocks
(`headnote`, `facts`, `decision`, `outcome`, `subproc`, `observation`, `source`) and the
metadata in a two-column `<strong>Label:</strong> | value` table (country of decision,
country of applicant, court name, date of decision, citation, additional citation, ECLI).

### 2026 site migration

The Drupal 7 site was replaced by a Laravel application, which is what broke the previous
scraper (issue #1299):

| Old (dead) | New |
|---|---|
| `/en/case-law-search?page=N` | 404 — replaced by `/summaries` + sitemaps |
| `/en/case-law/{title-slug}` | 301 to the site root — replaced by `/summaries/case/{slug}` |

Because the old case URLs no longer resolve and the new slugs are unrelated to the old
title-derived ones, record `_id`s changed (`EDAL-{new-slug}`). Rows ingested before the
migration will not be updated in place by this run.

## Usage

```bash
# Sample mode (15 documents)
python3 bootstrap.py bootstrap --sample

# Full fetch (~1,830 cases, ~1 hour at a 1s crawl delay)
python3 bootstrap.py bootstrap

# Fleet entry point (same as bootstrap, streams to data/records.jsonl)
python3 bootstrap.py bootstrap-fast

# Incremental (filters on decision date)
python3 bootstrap.py update

# Connectivity / parse check
python3 bootstrap.py test
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — ECRE/EDAL content licensed under Creative Commons Attribution 4.0.
