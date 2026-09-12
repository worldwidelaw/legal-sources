# INTL/CaribbeanCourt — Caribbean Court of Justice (CCJ) Judgments

Full-text judgments of the Caribbean Court of Justice:

- **Appellate Jurisdiction (AJ)** — final court of appeal for the CARICOM member
  states that have acceded to it (Barbados, Belize, Guyana, Dominica, …).
- **Original Jurisdiction (OJ)** — sole judicial body interpreting the Revised
  Treaty of Chaguaramas.

~303 distinct neutral citations from 2005 to present, born-digital PDFs.

## Access strategy

1. **WordPress media REST API** (`/wp-json/wp/v2/media?mime_type=application/pdf`)
   enumerates every PDF the site holds. `page` pagination is unreliable past
   ~130 items, so the scraper recurses on the upload-date range instead and
   halves any window whose `X-WP-Total` exceeds one page — every leaf query is
   page 1 only.
2. Filenames matching the neutral-citation pattern `YYYY_CCJ_N_(AJ|OJ)` are
   kept; non-English translations (`-Dutch`, `-French`) are dropped.
3. **Party names and delivery dates** are enriched from the two on-page Ninja
   Tables (appellate `table_id=8856`, original `table_id=18497`), matched on
   normalized citation.
4. **Internet Archive fallback.** The pre-2019 `caribbeancourtofjustice.org`
   uploads were never carried over to `ccj.org` — the media library still
   advertises them and the Ninja Tables still link to them, but the files
   themselves 404 today. Every URL variant of a judgment is tried in turn, and
   anything still dead falls back to the newest Wayback capture
   (`/web/{ts}id_/{url}`), indexed with two CDX queries for the whole run.

Judgments that are dead on the live site *and* absent from the Internet Archive
are logged by citation and skipped — that loss is upstream, not a scraper bug.

## Usage

```bash
python bootstrap.py test              # enumerate + report archive coverage
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap-fast     # concurrent full pull (fleet)
python bootstrap.py update             # incremental
```

## Record shape

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `title`, `text`, `date`,
`url`, `citation`, `jurisdiction_type`, `parties`, `state`, `keywords`,
`pdf_url`, `canonical_url`.

`date` is the delivery date; a candidate whose year disagrees with the neutral
citation's year is rejected (it is a cited case's date or a re-upload
timestamp), falling back to `YYYY-01-01`.

## Related sources

Overlaps `INTL/CCJ`, which reads the same ccj.org corpus through the Ninja
Tables only. This source additionally enumerates the media library and the
Internet Archive, so it recovers judgments whose on-page links are dead. A
merge of the two is pending an admin decision.

## License

[Public Domain — CCJ court records](https://ccj.org/) — official judgments of
the Caribbean Court of Justice, published for free public access on ccj.org.
Attribution to the Court is expected; commercial use is not restricted.
