# EC/AsambleaNacional — Ecuador Legislation (oficial.ec)

**Source:** [https://www.oficial.ec/](https://www.oficial.ec/)
**Data types:** legislation

Ecuadorian laws, decrees, ministerial agreements, resolutions and ordinances
published in the *Registro Oficial* and mirrored as full HTML on oficial.ec
(Drupal 7). ~6,200 documents from 2014 to present, Spanish.

## How it works

1. `/sitemap.xml?page=1` and `?page=2` list every node; non-document paths
   (`/temas/`, `/instituciones/`, index pages, …) are filtered out.
2. Each document page is fetched and the Drupal fields are concatenated into
   the full text: `field-header`, `field-considerando`, `body`, `field-firmas`.
3. Metadata comes from `field-insti` (institution), `field-ro-date`
   (Registro Oficial number + Spanish date) and the URL slug (document type).

## Crawl behaviour (issue #1316)

The crawl used to sleep 10 s inside `_parse_page` *and* another 10 s in
`BaseScraper.bootstrap()` (`rate_limit.requests_per_second: 0.1`), i.e. ~20 s
per document — about 34 h for the corpus, so fleet slots were torn down
mid-run with no way to resume.

- **Concurrency + pacing** — `WORKERS` threads (default 6) share one aggregate
  pacer of `RPS` requests/second (default 4). Pages answer in <1 s, so a full
  pull now takes ~26 min. Override with `EC_OFICIAL_WORKERS` / `EC_OFICIAL_RPS`.
- **Per-request timeout** — `(15 s connect, 45 s read)` with 2 retries, so one
  unresponsive page can no longer hang the run.
- **Checkpoint/resume** — every visited path (including 404s and fetch
  failures) is recorded in `data/checkpoint.json`, flushed every 100
  documents. A relaunched run skips them with no network calls, so repeated
  fleet slots advance monotonically. Sample runs do not write a checkpoint.

## Usage

```bash
python bootstrap.py test-api            # connectivity + single-page parse
python bootstrap.py bootstrap --sample  # 10 validation records
python bootstrap.py bootstrap --full    # full corpus (also: bootstrap-fast)
python bootstrap.py update              # sitemap lastmod >= last run
```

## License

Open government data — [https://www.oficial.ec/](https://www.oficial.ec/)
