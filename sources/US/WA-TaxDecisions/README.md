# US/WA-TaxDecisions — Washington Tax Decisions (WTD)

Appeals-division **Determinations** of the Washington State Department of Revenue,
published as **Washington Tax Decisions (WTD)** under RCW 82.32.410. These are the
Department's published quasi-adjudicative rulings on taxpayer appeals, applying
Washington's excise tax law (B&O tax, retail sales/use tax, etc.) to contested
facts. Each carries a `<vol> WTD <page>` citation (e.g. `45 WTD 080`).

This source is **distinct from US/WA-BTA** (the independent Board of Tax Appeals) —
WTDs are the DOR's *own* appeals-division determinations.

## Data type

`doctrine` — official interpretive determinations published by the state tax
authority.

## Source & method

- **Index:** `https://dor.wa.gov/washington-tax-decisions` — server-rendered HTML
  tables (one block per volume), no JavaScript, no CAPTCHA, no auth. ~776 PDF
  links (volumes ~30–45).
- **Documents:** born-digital text-layer PDFs at
  `/sites/default/files/<yyyy-mm>/<vol>WTD<page>.pdf`, extracted via
  `common.pdf_extract` (per-page flush_cache to avoid OOM). Occasional scanned
  attachments with no text layer are auto-skipped (<150-char guard).

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull
```

Sample run: 12 records, full text 7K–42K chars each, all with valid `<vol> WTD <page>`
citations and ISO dates.

## License

[Public Domain (US Government Work — 17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — Washington Tax Decisions are official Washington state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
