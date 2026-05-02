# Honduras Tax Authority SAR Guidance and Agreements (sar.gob.hn)

**Source:** [https://www.sar.gob.hn/](https://www.sar.gob.hn/)
**Country:** HN
**Data types:** doctrine
**Status:** Blocked

## Why this source is blocked

**Category:** PDF-only content (no text extraction available)

**Technical reason:** `no_api_pdf_only`

**Details:** WordPress site with broken WPDM download plugin (500 errors on file downloads). Legal documents (Acuerdos, Código Tributario) are PDFs behind WPDM. Comunicados are scanned image PNGs. WP REST API posts are short taxpayer notifications or institutional news — not tax doctrine. SSL certificate also invalid.

## How you can help

Documents are only available as PDFs requiring OCR or specialized extraction.
- If you have PyMuPDF/pdfplumber expertise, a PR to add extraction would help
- Scanned PDFs would need OCR (Tesseract)

- File an issue or open a PR at [worldwidelaw/legal-sources](https://github.com/worldwidelaw/legal-sources)
