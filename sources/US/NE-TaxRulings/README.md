# US/NE-TaxRulings — Nebraska Department of Revenue Revenue Rulings & GILs

Full text of the interpretive guidance published by the Nebraska Department of
Revenue (NDR):

- **Revenue Rulings** (`NN-NN-N`) — formal interpretations issued by the Tax
  Commissioner of how Nebraska tax law applies to a stated set of facts. A
  taxpayer that follows a current ruling has a **"safe harbor"** from tax,
  penalty and interest on the issue addressed.
- **General Information Letters** (`GIL NN-NN-N`) — less formal written
  statements of the Department's position on a topic.

Both are official state-government interpretive guidance (not adjudications of
a contested case), so the corpus is classified as `doctrine`.

## Source

- Revenue Rulings: <https://revenue.nebraska.gov/about/legal-information/revenue-rulings-issued-tax-commissioner>
- GILs: <https://revenue.nebraska.gov/about/legal-information/general-information-letters-gils>
- Document PDFs: `https://revenue.nebraska.gov/sites/default/files/doc/legal/rulings/<file>.pdf`

Each listing page is a server-rendered Drupal table with the columns
**Number | Tax Type | Title/Topic | Date | Document**. The Document cell links
to a born-digital PDF. Full text lives only in the PDF and is extracted via
the shared `common.pdf_extract` helper (pdfplumber → pypdf → OCR fallback).
No CAPTCHA, no JavaScript challenge, no auth (the host serves a browser
User-Agent at HTTP 200). The corpus is ~250 Revenue Rulings plus ~22 GILs.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction smoke test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # Full pull (~270 documents)
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
Revenue Rulings and General Information Letters of the Nebraska Department of
Revenue are official state-government works in the public domain under the
government-edicts doctrine. Commercial use permitted; no attribution required.
