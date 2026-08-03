# US/NY-TaxAppeals — New York State Division of Tax Appeals

Full text of the adjudicative output of the **New York State Division of
Tax Appeals (DTA)**:

- **Tax Appeals Tribunal decisions** (`*.dec.pdf`) — the Tribunal is the
  final administrative review body for NY State tax disputes.
- **Administrative Law Judge determinations** (`*.det.pdf`) — first-level
  adjudication of taxpayer petitions.
- **Orders** (`*.ord.pdf`) — procedural orders.

Each document resolves a specific tax controversy between a taxpayer and
the NY Department of Taxation and Finance under the Tax Law, so the
corpus is `case_law`.

## Source

Published openly at [dta.ny.gov](https://dta.ny.gov/) as born-digital,
text-layer PDFs. Three server-rendered HTML index pages list every
document as a direct PDF link — no JavaScript, no CAPTCHA, no auth:

- `/decisions/` — current Tribunal decisions
- `/determinations/` — current ALJ determinations
- `/pdf/archive/archive_index.htm` — full historical archive
  (~23,600 PDF links, 1986–present)

## How it works

1. GET each index page and collect every `*.pdf` link, absolute-ized and
   deduped by URL.
2. Parse the DTA docket number and document kind from the filename
   (e.g. `831624.dec.pdf` → DTA No. 831624, decision).
3. Download each PDF and extract its text layer via
   `common.pdf_extract`.
4. Derive the petitioner name (from the *Matter of the Petition of …*
   caption) and the decision date (from the `DATED:` line) out of the
   document text.
5. Normalize into the standard `case_law` schema.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all documents)
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity test
```

## License

[Public Domain (US Government Work — New York)](https://www.law.cornell.edu/uscode/text/17/105) — New York State Division of Tax Appeals decisions and determinations are official state government works in the public domain. No attribution required; commercial use permitted.
