# US/NJ-BPU — New Jersey Board of Public Utilities Orders

Full text of **Board Orders** issued by the **New Jersey Board of Public
Utilities (NJBPU)** adjudicating utility dockets across the electric,
natural-gas, water, telecommunications, cable-television and clean-energy
industries. Each Order is a final administrative adjudication of a specific
docket by the Board — **case_law**.

## Source

- **Publisher:** New Jersey Board of Public Utilities (NJBPU)
- **Public document search:** https://publicaccess.bpu.state.nj.us/Search.aspx
- **Type:** `case_law`
- **Auth:** none (public)

## How it works

`publicaccess.bpu.state.nj.us` is an ASP.NET WebForms document search behind
Imperva/Incapsula.

1. `GET /Search.aspx` establishes the session (Incapsula + `ASP.NET_SessionId`
   cookies) and provides the hidden `__VIEWSTATE` / `__EVENTVALIDATION`.
2. `POST /Search.aspx` an **Advanced Search** (`searchType=Advanced`,
   `AdvanceDocumentTitle=ORDER` — which matches the **ORDERS** document folder,
   `ListType=Document`, `OpenDateFrom`/`OpenDateTo` as `M/D/YYYY`). The server
   returns `/SearchDocResults.aspx`: a server-rendered GridView (30 rows/page)
   with the Docket #, Document Title, Folder, Description, Posted Date and a
   `DocumentHandler.ashx?document_id={id}` link.
3. The pager is a `__doPostBack` on `lbtnNext` reposting the results-page
   viewstate; `fetch_all()` walks the corpus one month at a time (newest first)
   back to the ~2000 floor and keeps only rows in the ORDERS folder.
4. `normalize()` downloads each Order PDF from `DocumentHandler.ashx` (same
   session) and extracts full text via `fitz`/PyMuPDF (Tesseract OCR fallback
   for the rare image-only scan).

The search endpoint intermittently returns a server-side 500 (redirect to
`Error.aspx`); every search re-GETs fresh tokens and retries.

**Note on duplicates:** a single Board Order at an agenda meeting can dispose of
several dockets and is filed once per docket in the ORDERS folder, so multiple
`document_id`s may point to the same PDF. Records are deduplicated on
`document_id` (each filing is a distinct document).

## Usage

```bash
python bootstrap.py test-api             # connectivity test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # high-throughput full pull (VPS)
```

## License

[Public Domain (US Government Work — New Jersey)](https://www.law.cornell.edu/uscode/text/17/105) — NJ Board of Public Utilities Orders are official state government edicts in the public domain. No attribution required; commercial use permitted.
