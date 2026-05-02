# Guatemala Congressional Legislation Database

**Source:** [https://www.congreso.gob.gt/](https://www.congreso.gob.gt/)
**Country:** GT
**Data types:** legislation
**Status:** Blocked

## Why this source is blocked

**Category:** PDF-only content (no text extraction available)

**Technical reason:** `waf_and_pdf_only`

**Details:** congreso.gob.gt blocked by Incapsula WAF (403). Alternative legal.dca.gob.gt has JSON API with 204 legislative decrees (2018+) but content is scanned PDF gazette pages requiring OCR. No structured full text available.

## How you can help

Documents are only available as PDFs requiring OCR or specialized extraction.
- If you have PyMuPDF/pdfplumber expertise, a PR to add extraction would help
- Scanned PDFs would need OCR (Tesseract)

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
