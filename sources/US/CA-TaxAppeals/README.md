# US/CA-TaxAppeals — California Office of Tax Appeals (Opinions)

Full text of California's **tax-appeal opinions** published by the Office
of Tax Appeals (OTA), rooted at
[ota.ca.gov/opinions](https://ota.ca.gov/opinions/).

Two eras, ~8,200 opinions in total:

| Era | Count | Coverage | Where |
|-----|-------|----------|-------|
| Legacy **State Board of Equalization** (SBE) precedential opinions — still binding precedent before the OTA | ~4,087 | 1930–2015 | the `/opinions/` TablePress listing |
| **OTA**'s own opinions, precedential and non-precedential | ~4,156 | 2018–present | ~35 per-year pages linked from that index |

Each opinion resolves a tax controversy between a taxpayer/appellant and
the Franchise Tax Board or the California Department of Tax and Fee
Administration (CDTFA), so the corpus is `case_law`.

## Source

`/opinions/` is a server-rendered **TablePress** listing: every legacy SBE
row and its `wp-content/uploads/.../*.pdf` link is present in the HTML,
and the DataTables widget only paginates client-side.

That page does **not** carry OTA's own 2018-present opinions. Those live
on separate per-year, per-tax-programme pages linked from it —
`/2026-franchise-income-tax-opinions/`, `/2026-business-tax-opinions/` and
their `precedential` variants, with older slugs such as `/2018-opinions/`
and `/2021-fit-opinions/`. Crawling only `/opinions/` is what left this
source frozen at 2015 (issue #1504).

No JavaScript, no CAPTCHA, no auth. All PDFs are born-digital with a real
text layer.

## How it works

1. GET the `/opinions/` HTML page (one request). It yields both the legacy
   SBE PDF rows and the links to every OTA year page.
2. GET each year page (~35) and collect its opinion PDFs.
3. Skip admin docs (org charts, errata notices, agendas) and dedup by URL.
4. Parse `{YY}-SBE-{NNN}` from legacy filenames; OTA opinions carry their
   `{YYYY}-OTA-{NNN}` citation and `OTA Case No.` in the PDF text.
5. Download each PDF and extract its text layer via `common.pdf_extract`.
6. Derive the appellant name and decision date from the document text.
7. Normalize into the standard `case_law` schema.

### Notes on identity and dates

- **`_id`** keys on the bare PDF filename for the legacy archive, whose
  opinion numbers are already unique. OTA-era filenames are *not* unique —
  17 of them repeat across upload folders (two different `J.-Parker.pdf`,
  for instance) — so those key on `{YYYY}-{MM}-{filename}` taken from the
  upload path. The legacy scheme is frozen so previously ingested rows
  keep their `_id`.
- **Dates** prefer OTA's `Date Issued:` stamp. It is a flattened form
  field, so extraction routinely splits a digit run with a stray space
  (`6 /4/2026`, `8/1/202 5`), and 2018–2019 opinions spell the month out
  with a blank day; all three shapes are handled. OTA opinions never fall
  back to a date scraped from the body — they cite many dates in their
  facts — and use the upload month instead.
- **`fetch_updates(since)`** skips year pages older than `since`, so an
  incremental refresh reads ~10 pages rather than re-crawling all 8,200
  PDFs only to discard them at the date filter.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all opinions -> data/records.jsonl)
python bootstrap.py bootstrap --sample   # 16 sample documents, both eras -> sample/
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py update --since DATE  # Incremental refresh
python bootstrap.py test-api             # Connectivity test
```

## License

[Public Domain (US Government Work — California)](https://www.law.cornell.edu/uscode/text/17/105) — California Office of Tax Appeals / State Board of Equalization opinions are official state government works in the public domain. No attribution required; commercial use permitted.
