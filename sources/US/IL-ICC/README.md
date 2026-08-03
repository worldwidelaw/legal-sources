# US/IL-ICC — Illinois Commerce Commission Orders

Full text of **Orders** issued by the **Illinois Commerce Commission (ICC)**,
the state agency that regulates Illinois public utilities. Each Order is the
Commission's administrative adjudication of a specific docket — rate cases,
complaints, certificates of service, reorganizations and other proceedings
across the electric, gas, water/sewer, telecommunications and transportation
(motor carrier) industries. These are binding decisions on the parties =
**case_law**.

## Source

- **Agency:** Illinois Commerce Commission
- **System:** ICC e-Docket (`icc.illinois.gov/docket`)
- **Coverage:** 2000 – present (electronic e-Docket era)
- **Corpus size:** ~21,700 "Order - Final" documents plus additional
  "Order", "Order on Rehearing" and "Order on Remand" filings.

## How it works

1. The public document search endpoint `/docket/search/documents/results` is a
   POST form. Posting `selectedDocumentType` + `SelectedYear` returns a
   server-rendered card list of every matching document for that year (type,
   description, party names, issue date, and a detail-page link). No login,
   cookie or anti-forgery token is required — only the interactive e-filing
   side needs an account.
2. `fetch_all()` walks years newest-first back to 2000, over the adjudicative
   order document types.
3. `normalize()` opens each document's detail page, downloads the primary
   born-digital Order PDF (`/docket/{case}/documents/{docid}/files/{fileid}.pdf`)
   and extracts the full text with PyMuPDF (Tesseract OCR fallback for the rare
   image-only scan).

## Usage

```bash
python bootstrap.py test-api             # Connectivity + full-text check
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # Full pull (all Orders)
python bootstrap.py bootstrap-fast       # High-throughput full pull (VPS)
```

## License

[Public Domain (US Government Work — Illinois)](https://www.law.cornell.edu/uscode/text/17/105) —
ICC Orders are official Illinois state government edicts in the public domain.
Commercial use permitted; no attribution required.
