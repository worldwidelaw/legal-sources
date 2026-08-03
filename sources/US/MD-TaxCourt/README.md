# US/MD-TaxCourt — Maryland Tax Court

Full text of the published decisions ("Memoranda of Grounds for Decision") of
the **Maryland Tax Court** — the independent state adjudicative body that hears
appeals from assessments and determinations on Maryland income tax, real- and
personal-property tax, inheritance/estate tax, sales/use tax and other state and
local taxes. Disputes run between taxpayers and the Comptroller of Maryland, the
State Department of Assessments and Taxation, a local Supervisor of Assessments,
or a Register of Wills, so the corpus is **case_law**.

## Source

Official site `https://taxcourt.maryland.gov/Decisions.shtml` — a single
server-rendered HTML page listing every published decision (≈2008–present, with
some earlier archive decisions back to ~1999) as a direct link to a born-digital
PDF under `/PDF/Decisions/`, annotated with a "Decided MM/DD/YYYY" date. No JS,
no CAPTCHA, no auth.

- **Enumeration:** one GET of `/Decisions.shtml`; the scraper parses each row's
  PDF href, the anchor case name (dropping the recurring "(archive)" note) and
  the Decided date. ~121 decision links.
- **Full text:** the PDF text layer via `common.pdf_extract` (curl browser UA,
  ~1 req/s). Modern decisions are born-digital and extract cleanly; a minority of
  older scans carry no text layer and are auto-skipped by the <150-char guard
  (recoverable on an OCR-capable host).
- **Docket number:** parsed from the PDF filename prefix (e.g. `25-IR-OO-0471`);
  older archive PDFs with non-standard filenames have no docket (optional field).

## Output schema

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `slug`, `docket_number`,
`court`, `case_name`, `title`, `text` (full decision), `url`, `date`
(ISO 8601), `jurisdiction` (`US-MD`).

## Usage

```bash
python3 bootstrap.py bootstrap --sample   # 12 sample decisions, newest first
python3 bootstrap.py bootstrap            # full pull (~121 decisions)
python3 bootstrap.py test-api             # connectivity / extraction test
```

## License

[Public Domain (US Government Work — Maryland)](https://www.law.cornell.edu/uscode/text/17/105) — Maryland Tax Court decisions are official state government works (judicial-type opinions / edicts of government) in the public domain. Commercial use permitted.
