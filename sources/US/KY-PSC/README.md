# US/KY-PSC — Kentucky Public Service Commission (Orders)

Full text of **Orders** issued by the Kentucky Public Service Commission (PSC)
adjudicating utility dockets — rate cases, certificate applications, complaints
and other proceedings across the electric, natural-gas, water/sewer and
telecommunications industries. Each Order is an administrative adjudication of a
specific case by the Commission = **case_law**.

## Source

- **Authority:** Kentucky Public Service Commission (PSC)
- **System:** "Order Vault", `psc.ky.gov`
- **Listing:** `https://psc.ky.gov/order_vault/Orders_{YYYY}/` (IIS directory browse)
- **Auth:** none (public)

## How it works

1. The PSC publishes every Order as a born-digital PDF under
   `/order_vault/Orders_{YYYY}/`. Each year folder is an IIS directory listing
   (a `<pre>` of `<a href>` links, one per Order PDF).
2. The PDF filename encodes the case number and order date:
   `{CCCCNNNNN}_{MMDDYYYY}[_NN].pdf` — the first 9 digits are the case number
   (4-digit case-year + 5-digit sequence, e.g. `202600097` → `2026-00097`), the
   next 8 digits are the order date, and an optional `_NN` suffix distinguishes
   multiple orders issued in the same case on the same day.
3. `fetch_all()` walks the year folders newest-first (back to ~1989) and yields
   one raw dict per Order PDF. `normalize()` downloads the PDF and extracts full
   text via `fitz`/PyMuPDF (Tesseract OCR fallback for the rare image-only
   scan). The authoritative case number is parsed from the Order body
   ("CASE NO. ...") with the filename code as fallback.

The corpus is large — roughly 1,500–1,800 Orders per year across ~40 years.
Orders are **born-digital PDFs** with a real text layer; no OCR is needed for
the vast majority.

## Usage

```bash
python bootstrap.py test-api            # Connectivity test
python bootstrap.py bootstrap --sample  # Fetch ~12 sample documents
python bootstrap.py bootstrap           # Full pull (all Orders)
python bootstrap.py bootstrap-fast      # High-throughput full pull (VPS)
```

## Record schema

`_id` (order PDF stem, primary key), `case_number`, `title`, `text` (full Order
text), `date` (ISO 8601), `url`, `pdf_url`.

## License

[Public Domain (US Government Work — Kentucky)](https://www.law.cornell.edu/uscode/text/17/105) — Kentucky Public Service Commission Orders are official state government edicts in the public domain. Commercial use permitted; no attribution required.
