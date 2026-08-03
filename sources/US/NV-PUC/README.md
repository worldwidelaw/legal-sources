# US/NV-PUC — Public Utilities Commission of Nevada (PUCN) Orders

Full text of **Orders** issued by the Public Utilities Commission of Nevada
(PUCN) adjudicating utility dockets — electric, natural-gas, water,
telecommunications, railroad and renewable/clean-energy proceedings, including
general rate cases, certificate/approval and integrated-resource-plan
applications, and complaints. Each Order is a final or procedural
administrative adjudication of a specific docket = **case_law**.

## Source

- **Publisher:** Public Utilities Commission of Nevada (PUCN)
- **Public document search:** https://puc-onbase.nv.gov/ (Hyland OnBase Public
  Access — Angular SPA over a public JSON API at `/api`)
- **Docket index (companion):** https://pucweb1.state.nv.us/puc2/Dktinfo.aspx?Util=All

## How it works

1. **Search.** POST `/api/CustomQuery/KeywordSearch` with the built-in public
   custom query *"PUC - Public Search - Dockets"* (`QueryID` 125), a Filing-Date
   window (`FromDate`/`ToDate` as `M/D/YYYY`) and `QueryLimit`. The response
   lists documents with an opaque OnBase `ID` and display columns
   `[Docket Number, Category, Filing Date, Description]`. Rows whose
   `Category == "ORDER"` are kept.
2. **Full text.** GET
   `/api/Document/{urlencoded-ID}/?ViewerMode=PDF&ForceDownload=true` (public,
   no auth) to download each Order PDF. PUCN Order PDFs are **scanned images**
   with no born-digital text layer, so full text is produced by rasterizing
   each page (fitz/PyMuPDF) and running **Tesseract OCR**.

`fetch_all()` walks the corpus one month at a time (newest first) back to the
~2002 floor. The stable record id is `US/NV-PUC/{docket}-{sha1(Name)[:12]}`.

## Usage

```bash
python bootstrap.py test-api             # connectivity + one full-text sample
python bootstrap.py bootstrap --sample   # ~12 sample Orders
python bootstrap.py bootstrap            # full pull
```

OCR requires `pytesseract` + Pillow and the `tesseract` binary on PATH.

## License

[Public Domain (US Government Work — Nevada)](https://www.law.cornell.edu/uscode/text/17/105) — PUCN Orders are official Nevada state government edicts in the public domain. No attribution required; commercial use permitted.
