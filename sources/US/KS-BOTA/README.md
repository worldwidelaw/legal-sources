# US/KS-BOTA — Kansas Board of Tax Appeals (BOTA)

Published decisions and orders of the **Kansas Board of Tax Appeals** (BOTA,
formerly the Court of Tax Appeals / COTA), the independent quasi-judicial agency
that adjudicates **all** Kansas tax appeals:

- Property valuation & equalization appeals (docket suffix `EQ` / `PV` / `PVX`)
- Division-of-Taxation income / sales / use determinations (`DT`)
- Tax-exemption applications (`TX` / `PR`)

Every published document decides a specific case → **`case_law`**, jurisdiction
`US-KS`. This is distinct from **US/KS-TaxRulings** (Kansas Department of Revenue
interpretive guidance = doctrine).

## Source

- **Index (curated / selected decisions):**
  <https://bota.kansas.gov/decisions-orders/published-decisions/> — a WordPress
  page that server-renders ~25 direct links to decision PDFs under
  `/wp-content/uploads/Selected_Decisions/`. Filenames encode the year, docket,
  and tax-type suffix (e.g. `2004_3806_EQ.pdf`, `2008-7226-EQ-Final-Order.pdf`,
  `2010-8538-PVX-Summary-Judgment.pdf`).
- **Full docket corpus (follow-up):** the complete set beyond the ~25 curated
  decisions lives behind the Grails/JSP search app at
  <https://www.kansas.gov/bota-search/> ("COTA-Search"), a form-POST search
  whose result rows link the order PDFs.

No JavaScript, no CAPTCHA, no authentication.

## Full text

Text is read from the decision PDFs via `common.pdf_extract`. A few decisions
are born-digital (text extracted directly); most are **scanned images** with no
text layer, so full text is produced by the OCR fallback (PyMuPDF →
`pytesseract`), which requires the **`tesseract`** binary on the host. OCR is
slow (~1–12 s per decision); a `<200`-char guard drops any PDF that still yields
no text.

## Usage

```bash
python bootstrap.py test-api                # smoke test
python bootstrap.py bootstrap --sample      # ~12 sample records
python bootstrap.py bootstrap --full        # all curated decisions
```

If `tesseract` is installed off-PATH (e.g. Homebrew at `/opt/homebrew/bin`),
prepend it: `export PATH="/opt/homebrew/bin:$PATH"`.

## License

[Public Domain — US Government Work (17 U.S.C. § 105)](https://www.law.cornell.edu/uscode/text/17/105) — published decisions and orders of the Kansas Board of Tax Appeals are official Kansas state-government works in the public domain under the government-edicts doctrine. Commercial use permitted; no attribution required.
