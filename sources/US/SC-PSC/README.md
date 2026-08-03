# US/SC-PSC — South Carolina Public Service Commission Orders

Full text of **Orders** issued by the **South Carolina Public Service Commission
(PSC)** adjudicating utility dockets across the electric, gas, water/sewer,
telecommunications and transportation industries, plus Hearing Officer
Directives on those dockets. Each Order is an administrative adjudication of a
specific docket → `case_law`.

- **Publisher:** South Carolina Public Service Commission
- **Portal:** https://dms.psc.sc.gov/Web/Orders (document management system)
- **Coverage:** late 1980s – present
- **Type:** `case_law`
- **Auth:** none

## How it works

1. `/Web/Orders/Search` is a GET form that returns a server-rendered HTML
   datatable of Orders filtered by an issue-date window (`StartDate`/`EndDate`
   as `M/D/YYYY`). Each row carries the Order number (e.g. `2024-1`, `2024-1H`),
   the industry, a summary (order title + docket description), the issue date
   and a direct attachment link `/Attachments/Order/{guid}` to the born-digital
   Order PDF.
2. The result set is capped at 1000 rows, so `fetch_all()` walks the corpus one
   month at a time (newest first) to stay under the cap. The modern PSC issues
   ~100–150 Orders/month at peak, well below the limit.
3. For each row, the Order PDF is downloaded and its full text extracted with
   PyMuPDF (`fitz`). PDFs are born-digital; the rare image-only scan is OCR'd
   with Tesseract. The docket number is parsed from the PDF body.

## Usage

```bash
python bootstrap.py test-api             # connectivity test
python bootstrap.py bootstrap --sample   # ~12 sample documents
python bootstrap.py bootstrap            # full pull (all Orders)
python bootstrap.py bootstrap-fast       # high-throughput full pull (VPS)
```

## Record schema

`order_number` (primary key), `docket_number`, `industry`, `title`, `summary`,
`text` (full Order text), `date` (ISO 8601), `url`, `pdf_url`.

## License

[Public Domain (US Government Work — South Carolina)](https://www.law.cornell.edu/uscode/text/17/105) — South Carolina Public Service Commission Orders are official state government edicts in the public domain. Commercial use permitted; no attribution required.
