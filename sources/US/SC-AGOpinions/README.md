# US/SC-AGOpinions — South Carolina Attorney General Opinions

Full text of legal opinions issued by the **South Carolina Office of the
Attorney General**, published openly at [scag.gov](https://www.scag.gov/opinions/opinions-archive/).
Each opinion answers a legal question posed by a public official (legislators,
county/municipal officials, state agencies) and constitutes an authoritative
advisory interpretation of South Carolina law (**doctrine**).

## Source

- **Archive:** `https://www.scag.gov/opinions/opinions-archive/`
- Paginated by year: `?year=YYYY&page=N` (up to 50 opinions per page; the
  on-page year filter exposes **1972–present**).
- Each opinion has a detail page at `/opinions/opinions-archive/{slug}/`
  carrying the title, issue date (`<span class="date">`), and a link to the
  opinion **PDF** (`/wp-content/uploads/...` or `/media/...`).

## Full text

Full text lives in the linked PDF. Extraction uses the shared, OOM-hardened
`common.pdf_extract` helper (pdfplumber → pypdf → OCR fallback). The historical
corpus (~1972–2018) consists of text-based PDFs and extracts cleanly; the most
recent opinions are image-only scans, which are skipped where OCR is
unavailable.

## Schema (doctrine)

`_id`, `_source`, `_type`, `_fetched_at`, `title`, `text`, `url`, `pdf_url`, `date`.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all years -> data/records.jsonl)
```

## License

[Public Domain (US Government Work — South Carolina)](https://www.law.cornell.edu/uscode/text/17/105) — South Carolina Attorney General opinions are official state government works in the public domain. No attribution required; commercial use permitted.
