# US/NE-PSC — Nebraska Public Service Commission Orders

Full text of **Orders** issued by the Nebraska Public Service Commission
(NPSC) adjudicating dockets across telecommunications, natural gas, grain
warehouse / grain dealer matters, manufactured / modular housing,
transportation (household-goods and passenger carriers), State 911 and
administrative rules-and-regulations proceedings. Each Order is an
administrative adjudication / edict of a specific docket → **case_law**.

## Source

- Order Search: https://www.nebraska.gov/psc/ordersearch/user/index.cgi
- Agency: https://psc.nebraska.gov/
- Documents: born-digital PDFs at `https://www.nebraska.gov/psc/orders/{subdir}/{file}.pdf`
  (subdirs: `telecom`, `ntips`, `natgas`, `grain`, `housing`, `tran`, `admin`, …)

Records span **May 27, 1980 → present** (the search floor). The corpus is
roughly **14,400 Orders**.

## How it works

1. The NPSC Order Search `/psc/ordersearch/user/index.cgi` exposes a
   keyword full-text index. A `POST` with `sbkw={keyword}` returns
   server-rendered result "cards", each carrying a direct link to the
   Order PDF plus a text snippet. Pagination is an AJAX `POST` with
   `page`/`size`/`next` params (10 hits/page, server-fixed size).
2. Every genuine NPSC Order PDF carries the "NEBRASKA PUBLIC SERVICE
   COMMISSION" header, so the broad keyword **`commission`** acts as the
   site's own full-text index over the whole corpus. `fetch_all()` walks
   the result pages and yields one raw dict per Order PDF (de-duplicated
   by PDF path).
3. `normalize()` downloads each Order PDF and extracts full text via
   `fitz`/PyMuPDF (Tesseract OCR fallback for the rare image-only scan).
   The **entered date** and **docket number** are parsed from the PDF
   body / filename.

Directory browsing of the `/psc/orders/` subdirectories is disabled (403),
so the keyword index is the enumeration path; individual PDF files are
served openly (200).

## Output fields

`doc_key` (PDF path without extension, primary key), `docket_number`,
`department`, `title`, `text` (full order text), `date` (ISO 8601, parsed
from the body), `url` / `pdf_url`.

## Run

```bash
python bootstrap.py test-api            # connectivity + one full-text order
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap-fast      # full pull (VPS)
```

## License

[Public Domain (US Government Work — Nebraska)](https://www.law.cornell.edu/uscode/text/17/105) — Nebraska Public Service Commission Orders are official state government edicts and carry no copyright. Commercial use permitted; no attribution required.
