# US/AFCCA — U.S. Air Force Court of Criminal Appeals

Full text of the **opinions and orders** of the **United States Air Force
Court of Criminal Appeals (AFCCA)**.

AFCCA is the intermediate military appellate court that reviews Air Force and
Space Force courts-martial (findings and sentence) under **Article 66, UCMJ**.
Each opinion/order resolves a specific court-martial appeal — i.e.
**case_law**. Its decisions are in turn reviewable by the **U.S. Court of
Appeals for the Armed Forces** (see **US/CAAF**).

- **Publisher:** U.S. Air Force Court of Criminal Appeals
- **Coverage:** ~2002–present, born-digital PDFs
- **Type:** `case_law`
- **Full text:** yes — PDF text layer via `common.pdf_extract` (no OCR)

## Source & method

- **Index:** per-year index pages
  `https://afcca.law.af.mil/opinions_date_{year}.html`, each listing that
  year's decisions as direct PDF links. Crawled newest year first, back to
  2002.
- **Documents:** born-digital decision PDFs at
  `https://afcca.law.af.mil/afcca_opinions/{cat}/{name}_-_{docket}_...pdf`.
- **URL resolution (fix):** the index-page hrefs are **root-relative**
  (`afcca_opinions/{cat}/{name}.pdf`) and are resolved against the **site
  root** (`https://afcca.law.af.mil/`), not against the index-page URL.
  Joining against `opinions_date_{year}.html` produced 404 URLs like
  `.../opinions_date_2026.html/afcca_opinions/x.pdf`.
- **Index flakiness:** the `opinions_date_{year}.html` pages are intermittent
  and occasionally 404; a missing/failed index year is skipped and the crawl
  continues.
- **Docket:** parsed from the PDF body (`ACM 40809`, `ACM S32806`,
  `Misc. Dkt. No. 2025-16`).
- **Date:** the `Decided DD Month YYYY` / `Decided Month DD, YYYY` line in the
  body.
- **Party:** the caption party after `v.`, with the PDF filename as a fallback.

No JavaScript, CAPTCHA or authentication is required.

## Fields

`_id`, `_source`, `_type=case_law`, `slug`, `docket_number`, `court`,
`title`, `text` (full decision), `url`, `date`, `year`, `jurisdiction=US`.

## Distinct from

- **US/CAAF** — U.S. Court of Appeals for the Armed Forces (the reviewing
  court above AFCCA).

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (all decisions)
```

## License

[Public Domain (U.S. federal government work)](https://www.law.cornell.edu/uscode/text/17/105)
— decisions of the U.S. Air Force Court of Criminal Appeals are works of the
United States federal government, not subject to copyright under 17 U.S.C.
§ 105, and judicial opinions are public domain under the government-edicts
doctrine. Commercial use permitted; no attribution required.
