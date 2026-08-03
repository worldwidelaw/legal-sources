# US/WI-TaxAppeals — Wisconsin Tax Appeals Commission

Published **Rulings & Orders** of the [Wisconsin Tax Appeals Commission](https://taxappeals.wi.gov/),
the independent state agency that adjudicates all Wisconsin state tax disputes
(income/franchise, sales & use, and property/manufacturing). Every published
document decides a specific case → **`case_law`**. The corpus includes the
Commission's own decisions plus the reviewing Court of Appeals / Circuit Court /
Supreme Court opinions filed alongside them. Decisions go back to 1984.

## Access

- **Discovery:** 26 server-rendered alphabetical index pages —
  `https://taxappeals.wi.gov/Pages/Decision%20Pages/All/{A..Z}.aspx` — each
  shipping direct `<a href="/Documents/....pdf">` links in the static HTML
  (no JavaScript, no CAPTCHA, no auth).
- **Metadata:** encoded in each filename — petitioner, docket (e.g. `20.I.188`,
  `07.T.141`), decision date (`MMDDYY` token), and authoring body
  (`TAC` = Commission; `CT APP` / `CIRCUIT` / `SUPREME` = reviewing court).
- **Full text:** the decision PDFs are **scanned images with no text layer**,
  so full text is obtained via the OCR fallback in `common.pdf_extract`
  (PyMuPDF/pdf2image → pytesseract), which requires the `tesseract` binary.

## Status

`planned` — discovery, filename parsing (date/docket/court), and PDF download
are all verified working. **OCR is required for full text**; the `tesseract`
binary is absent on the build machine, so full-text samples cannot be produced
here. Launch on a vantage that has tesseract installed, confirm full text, then
mark `complete`. Budget for OCR slowness across the multi-thousand-decision
corpus (consider per-letter/era partitioning to fit a fleet slot).

## Usage

```bash
python bootstrap.py test-api             # Connectivity + extraction test (needs tesseract)
python bootstrap.py bootstrap --sample   # Fetch ~12 samples
python bootstrap.py bootstrap            # Full pull
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
Rulings & Orders of the Wisconsin Tax Appeals Commission and the reviewing
Wisconsin courts are official state-government works in the public domain under
the government-edicts doctrine. Commercial use permitted; no attribution required.
