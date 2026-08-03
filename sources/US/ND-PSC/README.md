# US/ND-PSC — North Dakota Public Service Commission Orders

Full text of **Orders** issued (and adopted) by the North Dakota Public
Service Commission (NDPSC) in its formal cases: utility rate and
certificate proceedings, grain warehouse / grain buyer matters, coal
reclamation (RC) permits, pipeline siting, telecommunications, weights &
measures, and other regulated matters. Each Order is an administrative
adjudication of a specific case docket → **case_law**.

## Source

- Case portal: https://www.psc.nd.gov/case-search
- Search app: https://apps.psc.nd.gov/cases/psdocketsearch
- Documents: born-digital PDFs at
  `https://www.psc.nd.gov/webdocs/case/{YY-NNNN}/{docket:03d}-{file:03d}.pdf`

## How it works

1. `POST /cases/psdocketsearch` with `docketTypeCode=Order` and a
   `filedFromDate`/`filedToDate` window. The date inputs are HTML
   `<input type=date>`, so the value **must** be `yyyy-mm-dd` (an
   `M/D/YYYY` value returns HTTP 500).
2. The response is an HTML results table: one row per Order docket with
   Case Number, docket number + description, page count, "On Behalf Of"
   (= `Public Service Commission` for a genuine Commission Order, or an
   ALJ/court name for adjudicative orders issued in the case), "Filed By",
   and Date Filed (`YYYY.MM.DD`). The result set is capped at ~100 rows
   with no pager, so `fetch_all()` walks **month windows** newest-first
   back to ~2000 to stay under the cap.
3. Each row links to `/cases/psdocketdetail?getId=&getId2=&getId3=`, whose
   page embeds the direct `webdocs` PDF link(s). The primary document is
   the `-010` file.
4. Full text is extracted from the PDF with `fitz`/PyMuPDF; the minority of
   image-only scans are OCR'd with Tesseract.

## Usage

```bash
python bootstrap.py test-api            # connectivity test
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # high-throughput full pull (VPS)
```

## License

[Public Domain (US Government Work — North Dakota)](https://www.law.cornell.edu/uscode/text/17/105) — North Dakota Public Service Commission Orders are official state government edicts in the public domain. No attribution required; commercial use permitted.
