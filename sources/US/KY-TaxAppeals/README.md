# US/KY-TaxAppeals — Kentucky Board of Tax Appeals, Final Orders

Full text of the **Final Orders** issued by the **Kentucky Board of Tax Appeals
(BTA)** — the Commonwealth's independent quasi-judicial tax tribunal, now housed
in the Public Protection Cabinet's **Office of Claims and Appeals (OCA)**.

The Board adjudicates:
- **Revenue** appeals — taxpayer challenges to Department of Revenue assessments,
  refund denials, and rulings on state taxes (income, sales & use, etc.).
- **Property** appeals — challenges to county Property Valuation Administrator
  (PVA) / Board of Assessment Appeals property-tax valuations and classifications.

Each Final Order (order of dismissal, agreed order of dismissal, or merits
ruling) finally resolves a specific contested case, so records are typed
`case_law`.

## Source

- Case search (ASP.NET WebForms): <https://kycc.ky.gov/claims/search.aspx>
- Final Order PDFs: `https://kycc.ky.gov/claims/FinalOrder/{APPEAL-NO}-{DOCID}.pdf`

## Method

1. GET the search page once; capture the ASP.NET hidden fields
   (`__VIEWSTATE`, `__VIEWSTATEGENERATOR`, `__EVENTVALIDATION`).
2. For each tax year in the `TxYear` dropdown (2021–2026 as of 2026-07), POST a
   Search; the server re-renders the full results table (no pagination).
3. Parse each row: pair every "View Final Order" PDF link with its metadata
   (Tax Year, County, Appeal Type, Status, Tax Payer Name, K#, Date Filed).
4. Download each PDF and extract text via the shared `common.pdf_extract` helper
   (pdfplumber → pypdf → OCR). Some orders are scanned, so OCR (tesseract) is
   used for that subset.

No authentication, no CAPTCHA. Rate limited to ~1 request/second.

## Fields

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `appeal_no`, `tax_year`,
`tax_type`, `county`, `status`, `taxpayer`, `issuer`, `title`, `text`
(full order text), `url`, `date` (Date Filed, ISO 8601), `jurisdiction`
(`US-KY`).

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105 (US government edicts)](https://www.law.cornell.edu/uscode/text/17/105) — Final Orders of the Kentucky Board of Tax Appeals are official Kentucky state-government adjudicative works, published for public inspection, in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
