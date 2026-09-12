# US/AR-PSC — Arkansas Public Service Commission Orders

Full text of **Orders** issued by the Arkansas Public Service Commission
(APSC) adjudicating utility dockets — electric, natural gas, water/sewer and
telecommunications rate cases, certificates of public convenience and
necessity (CCN), fuel and purchased-gas adjustment riders, formula-rate
reviews, tariff filings and rulemaking. Each Commission (or Administrative
Law Judge) Order is an administrative adjudication / edict of a specific
docket → **case_law**.

## Source

- Docket search: https://apps.apsc.arkansas.gov/olsv2/docket_search.asp
- Agency: https://apsc.arkansas.gov/
- Documents: born-digital PDFs served by the APSC Online Services (OLS v2)
  eFiling system at `https://apps.apsc.arkansas.gov/olsv2/viewdoc/pdfview.asp`.

Dockets are addressed `YY-NNN-X` (e.g. `22-064-U`, `07-016-U`). The legacy
`apscservices.info` eFiling host now 302-redirects to this OLS v2 system.

## How it works

1. **Docket enumeration.** The `docket_search.asp` `CaseNumber` `<select>`
   is pre-populated with several hundred recently-active dockets, which seed
   enumeration. Any other historic docket number also resolves directly, so
   `fetch_all()` additionally sweeps the `YY-NNN-X` id space per
   year/suffix with gap tolerance (a non-existent docket returns a short
   empty shell page with no `DocNumVal` links).
2. **Order rows.** `docket_search_results.asp?casenumber={docket}` returns
   the full filing log. Commission/ALJ Orders are the rows whose description
   begins `N. ORDER NO. M (COMMISSION)` (or an ALJ surname). `fetch_all()`
   yields one raw dict per Order row.
3. **Full text.** `Docket_Search_Documents.asp?Docket={docket}&DocNumVal={n}`
   lists the PDF part(s) as `viewdoc/pdfview.asp?document={file}.pdf`.
   `normalize()` downloads each part (raw `application/pdf`) and extracts
   full text via `fitz`/PyMuPDF (Tesseract OCR fallback for the rare scanned
   order), concatenating multi-part orders. The file/issued date is taken
   from the filing log (fallback: the PDF body).

## Output fields

`docket_number` (YY-NNN-X), `doc_number` (DocNumVal), `order_number`,
`authority` (COMMISSION / ALJ), `title`, `text` (full order text), `date`
(ISO 8601), `url` / `pdf_url`.

## Run

```bash
python bootstrap.py test-api            # connectivity + one full-text order
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap-fast      # full pull (VPS)
```

## License

[Public Domain (US Government Work — Arkansas)](https://www.law.cornell.edu/uscode/text/17/105) — Arkansas Public Service Commission Orders are official state government edicts and carry no copyright. Commercial use permitted; no attribution required.
