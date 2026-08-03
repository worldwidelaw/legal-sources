# US/ME-PUC — Maine Public Utilities Commission Orders

Full text of **Orders** issued by the **Maine Public Utilities Commission
(MPUC)** adjudicating utility dockets across the electric, natural-gas, water
and telecommunications industries. Each Order, Procedural Order or Protective
Order is an administrative adjudication of a specific docket by the Commission —
**case_law**.

## Source

- **Publisher:** Maine Public Utilities Commission (MPUC)
- **Public case-management portal:** https://mpuc-cms.maine.gov/CQM.Public.WebUI/
- **Type:** `case_law`
- **Auth:** none (public)

## How it works

`mpuc-cms.maine.gov/CQM.Public.WebUI` is a Hyland-style `CQM.Public.WebUI`
ASP.NET WebForms case-management app behind Imperva/Incapsula (the same family
as US/NJ-BPU and US/WI-PSC).

1. `GET /CQM.Public.WebUI/` establishes the session (Incapsula `visid_incap` +
   `incap_ses` cookies and the ASP.NET `__VIEWSTATE` / `__EVENTVALIDATION`
   tokens).
2. A `POST` with `__EVENTTARGET=…lbadvancedsearch` reaches
   `/Common/AdvanceSearch.aspx`; a further `POST` switches the radio group
   `rdblstMatterDocument=DOC` into **Documents** search mode.
3. A `POST btnTopSearch=Search` with a Date-Filed window
   (`txtDateFiledFrom` / `txtDateFiledTo` as `M/D/YYYY`) returns the
   server-rendered results grid (`grdSearchedPublicDocument`): one row per
   document carrying the Docket #, Case Title, Filing #, Filed Date, Document
   Type, Company and Document Title. Only Order-variant rows are kept.
4. The grid pages via a `__doPostBack` on `LnkNext`; `fetch_all()` walks the
   corpus one month at a time (newest first) back to the ~2000 floor.
5. Each document is opened via `HdnDocumentParameter` + `HdnBtnDocumentOpen`,
   then `GET /Common/ViewDoc.aspx` streams the born-digital Order PDF (or the
   occasional `.docx`). Full text is extracted via `fitz`/PyMuPDF (Tesseract OCR
   fallback for the rare image-only scan) or from the `.docx`
   (`word/document.xml`).

## Usage

```bash
python bootstrap.py test-api             # connectivity test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # high-throughput full pull (VPS)
```

## License

[Public Domain (US Government Work — Maine)](https://www.law.cornell.edu/uscode/text/17/105) — Maine Public Utilities Commission Orders are official state government edicts in the public domain. No attribution required; commercial use permitted.
