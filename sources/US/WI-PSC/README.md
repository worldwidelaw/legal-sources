# US/WI-PSC — Public Service Commission of Wisconsin (Orders)

Full text of **Orders** and **ALJ Orders** issued by the Public Service
Commission of Wisconsin (PSC) adjudicating utility dockets — rate cases,
certificate of authority (CE/CW) applications, complaints and other
proceedings across the electric, natural-gas, water and telecommunications
industries. Each Order is an administrative adjudication of a specific docket
by the Commission (or by an Administrative Law Judge) = **case_law**.

## Source

- **Authority:** Public Service Commission of Wisconsin (PSC)
- **System:** Electronic Records Filing (ERF), `apps.psc.wi.gov`
- **Search:** `https://apps.psc.wi.gov/ERF/ERFsearch/default.aspx`
- **Document view:** `https://apps.psc.wi.gov/ERF/ERFview/viewdoc.aspx?docid={docid}`
- **Auth:** none (public)

## How it works

1. The ERF advanced search is an ASP.NET WebForms page. A POST filtering by
   document type (checkbox `ckb_doc_type_cd$21` = "Order", `$1` = "ALJ Order")
   and a received-date window (`datepicker_start` / `datepicker_end` as
   `M/D/YYYY`) returns a server-rendered HTML results table: one row per
   document carrying the PSC Ref# (docid), description, document type,
   docket/utility id and received date.
2. The hidden `__VIEWSTATE` / `__EVENTVALIDATION` captured once from the search
   page are reused across every window POST (the control set is static), so
   `fetch_all()` walks the corpus one month at a time (newest first) back to
   the ERF floor.
3. `normalize()` downloads each Order PDF directly from
   `/ERF/ERFview/viewdoc.aspx?docid={docid}` and extracts full text via
   `fitz`/PyMuPDF (Tesseract OCR fallback for the rare image-only scan). The
   docket number falls back to a parse of the PDF body when the listing lacks
   one.

Orders are **born-digital PDFs** with a real text layer — no OCR is needed for
the modern corpus.

## Usage

```bash
python bootstrap.py test-api            # Connectivity test
python bootstrap.py bootstrap --sample  # Fetch ~12 sample documents
python bootstrap.py bootstrap           # Full pull (all Orders)
python bootstrap.py bootstrap-fast      # High-throughput full pull (VPS)
```

## Record schema

`psc_ref` (docid, primary key), `docket_number`, `doc_type`, `title`,
`description`, `text` (full Order text), `date` (ISO 8601), `url`, `pdf_url`.

## License

[Public Domain (US Government Work — Wisconsin)](https://www.law.cornell.edu/uscode/text/17/105) — Wisconsin Public Service Commission Orders are official state government edicts in the public domain. Commercial use permitted; no attribution required.
