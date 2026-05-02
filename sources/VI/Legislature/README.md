# USVI Legislature Acts & Code

**Source:** [https://billtracking.legvi.org/](https://billtracking.legvi.org/)
**Country:** VI
**Data types:** legislation
**Status:** Blocked

## Why this source is blocked

**Category:** Scanned PDFs requiring OCR

**Technical reason:** `scanned_pdfs_require_ocr`

**Details:** Bill tracking API (billtracking.legvi.org) has excellent JSON metadata for 9000+ bills across legislatures 25-36, but all Act PDFs are scanned images (JBIG2) with no text layer. Justia behind Cloudflare, Lexis requires JS, legvi.org/vi-code times out. Requires tesseract/OCR on VPS to extract full text from PDFs.

## How you can help

Documents are scanned images in PDF format, requiring OCR.
- Tesseract OCR integration would be needed
- If you have OCR expertise, a PR would be welcome

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
