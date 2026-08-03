# US/WV-TaxAppeals — West Virginia Office of Tax Appeals (Decisions)

Redacted final decisions of the **West Virginia Office of Tax Appeals** (OTA),
the independent state tribunal that adjudicates all West Virginia state tax
disputes — consumer sales & use, personal income, corporate net income,
business franchise, severance, and ad valorem / property tax. Every published
document decides a specific taxpayer appeal, so the whole corpus is
`case_law`.

After a decision becomes final it is redacted and filed in the State Register
maintained by the **West Virginia Secretary of State**, who publishes the
searchable corpus online.

## Source

- **Index (results page):** https://apps.sos.wv.gov/adlaw/taxappealdecisions/
- **Decision PDF:** `https://apps.sos.wv.gov/adlaw/taxappealdecisions/readpdf.aspx?did=N`
- **Publisher:** West Virginia Secretary of State — Administrative Law (State
  Register) on behalf of the WV Office of Tax Appeals
- **Coverage:** ~591 redacted final decisions

## Access method

The results page is fully **server-rendered** (no JavaScript, no CAPTCHA, no
auth). Each decision is one `<tr>` carrying the docket number, the *Date
Issued* (`M/D/YYYY`), a `readpdf.aspx?did=N` link to the decision PDF, and a
description row summarising the appeal. The scraper parses those rows,
downloads each PDF, and extracts the full text.

**Text layer:** the newest decisions are **born-digital** (extracted directly
via pdfplumber); the older majority are **scanned images** with no text layer.
`common.pdf_extract` extracts born-digital text directly and falls back to OCR
(PyMuPDF/pdf2image → pytesseract) for the scanned ones, which requires the
`tesseract` binary. Launch on a vantage with `tesseract` installed to obtain
full text for the scanned majority; budget for OCR slowness over ~591 PDFs. A
200-character guard skips any PDF that still yields no text.

## Fields

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `slug`,
`docket_number`, `court`, `title`, `summary`, `text` (full decision), `url`,
`date` (ISO 8601, from *Date Issued*), `jurisdiction` (`US-WV`).

## Usage

```bash
python bootstrap.py test-api            # Connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # Full pull
python bootstrap.py bootstrap-fast      # Alias for full pull (VPS wrapper)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
— redacted final decisions of the West Virginia Office of Tax Appeals, filed in
the State Register and published by the WV Secretary of State, are official
state-government works in the public domain under the government-edicts
doctrine. Commercial use permitted.
