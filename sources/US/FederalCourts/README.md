# US/FederalCourts — US Federal Courts (SCOTUS + Circuits)

**Source:** [https://www.courtlistener.com](https://www.courtlistener.com)
**Data types:** case_law

Opinions of the Supreme Court of the United States and all 13 federal courts of
appeals, via the CourtListener REST API (v4).

## Access path

1. **Search API** — `GET /api/rest/v4/search/?type=o&court={id}` — public, no auth.
   Provides enumeration and all metadata (case name, docket, date, citation).
2. **Opinions API** — `GET /api/rest/v4/opinions/{id}/` — **requires a token**.
   Provides stored full text (`html_with_citations`, `plain_text`, `xml_harvard`, …).
3. **Stored files** — `https://storage.courtlistener.com/{local_path}` — public.
   PDF/HTML of the filed opinion; extracted with `common/pdf_extract`.

The scraper tries (2) when `COURTLISTENER_API_TOKEN` is set and falls back to (3).
Set the token in `.env` to reach the pre-1950 corpus (see coverage below).

The CourtListener **website** (as opposed to the API) sits behind an AWS WAF that
answers HTTP 202 with an empty body, so HTML scraping of opinion pages is not a
viable fallback.

## Enumeration (issue #1494)

The corpus is swept **per court, per year, newest year first**, and a checkpoint
is written to `data/checkpoint.json` after each completed `(court, year)` window.

This replaced a single flat `dateFiled desc` cursor walk that restarted from the
newest opinion on every run. Because nothing was checkpointed, every fleet run
re-crawled the same recent opinions and hit the wall-clock cap before advancing —
which is why only 8,583 rows (2024–2026) were indexed out of ~1.4M available.

Courts are swept in the order `ca2, ca9, ca1, ca3…ca11, cadc, cafc, scotus`;
CA2 and CA9 lead because issue #1494 tracks them explicitly, and SCOTUS is last
because it is by far the largest single court.

## Coverage

Available opinions per the live search API (measured 2026-08-25):

| Court | Available | Earliest |
|-------|-----------|----------|
| SCOTUS | 498,145 | 1759 |
| CA9 (Ninth Circuit) | 137,977 | 1854 |
| CA2 (Second Circuit) | 88,358 | 1820 |
| all 14 courts | ~1.4M | — |

Full-text availability is **era-dependent**, and the cutoff is much later than it
first appears. Sampling 20 opinions per year (measured 2026-08-25):

| Year | CA2 `local_path` | CA9 `local_path` | dead-URL-only |
|------|------------------|------------------|---------------|
| 1960 | 0/20 | 0/20 | 12–16 |
| 1980 | 0/20 | 0/20 | 16–18 |
| 1995 | 0/20 | 0/20 | 14–19 |
| 2003 | 0/20 | 0/20 | 3–7 |
| 2012 | 15/20 | 13/20 | 0 |
| 2022 | 20/20 | 20/20 | 0 |

**`local_path` — a real file on storage.courtlistener.com — only exists from
roughly 2005 onward.** Older clusters carry either nothing at all (the Harvard
CAP import, mostly pre-1950) or a `download_url` pointing at
`bulk.resource.org`, the Public.Resource.Org bulk dump host, which **no longer
resolves**. A naive reading of `download_url` therefore overstates token-free
coverage badly; the scraper treats those hosts as absent (`DEAD_FILE_HOSTS`)
rather than attempting and misreporting them as extraction failures.

Consequence: **without `COURTLISTENER_API_TOKEN` the reachable corpus is
approximately 2005–present.** Everything older is API-only and is counted under
`skipped_no_stored_text` in the coverage report — **reported, not silently
dropped**. The previous implementation dropped them silently, which is the second
half of the #1494 root cause.

`data/coverage_report.json` records, per court: opinions available in the windows
swept, fetched-with-full-text, and each skip reason separately, so per-circuit
coverage is independently auditable.

Run `python bootstrap.py coverage` for a live expected-count audit per court.

## Usage

```bash
python bootstrap.py test                  # connectivity
python bootstrap.py bootstrap --sample    # 15 samples across courts/eras
python bootstrap.py bootstrap --full      # checkpointed backfill (resumable)
python bootstrap.py bootstrap-fast --full # same, VPS wrapper entrypoint
python bootstrap.py update --since 2026-01-01
python bootstrap.py coverage              # per-court expected counts
```

## Rate limits

The anonymous search API throttles aggressively (HTTP 429 after roughly a dozen
rapid requests). Search calls are spaced by `fetch.search_delay` (2s) and 429s
back off up to 60s, honouring `Retry-After`. An API token raises the ceiling to
5,000 queries/hour.

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — US
federal court opinions are not subject to copyright. CourtListener metadata is
published by the Free Law Project under [CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/).
