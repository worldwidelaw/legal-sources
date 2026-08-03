# US/CO-TaxRulings — Colorado Department of Revenue Letter Rulings

Full text of the **Letter Rulings** published by the Colorado Department of
Revenue, Taxation Division:

- **Private Letter Rulings** (`PLR-YY-###`) — the Department's written
  determination of how Colorado tax law applies to a specific taxpayer's
  facts; **binding** on the Department as to the requesting taxpayer under
  1 CCR 201-1, Rule 24-35-103.5.
- **General Information Letters** (`GIL-YY-###`) — general, **non-binding**
  statements of the Department's interpretation of Colorado tax law.

Both are official state-government interpretive guidance (not adjudications
of a contested case), so the corpus is classified as `doctrine`.

## Source

- Index: <https://tax.colorado.gov/all-letter-rulings>
- Document PDFs: `https://tax.colorado.gov/sites/tax/files/documents/<NUM>.pdf`

The "All Letter Rulings" page is a single server-rendered Drupal table. Each
row carries the ruling number, an anchor to the public PDF, the title, a
"Published &lt;date&gt;" clause and a short description. Rescinded rulings
appear as rows without a PDF and are skipped. Full text lives only in the
PDF and is extracted via the shared `common.pdf_extract` helper
(pdfplumber → pypdf → OCR fallback).

`tax.colorado.gov` returns HTTP 403 to non-browser User-Agents; the scraper
sends a desktop-browser UA and is served 200. No CAPTCHA, no JavaScript
challenge (nginx, not Cloudflare).

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction smoke test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (~360+ rulings)
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
Private Letter Rulings and General Information Letters of the Colorado
Department of Revenue are official state-government works in the public
domain under the government-edicts doctrine. Commercial use permitted; no
attribution required.
