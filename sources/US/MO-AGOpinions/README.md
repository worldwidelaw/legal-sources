# US/MO-AGOpinions — Missouri Attorney General Opinions

Full text of legal opinions issued by the **Missouri Attorney General's
Office**. Each opinion answers a legal question posed by a public official
and constitutes an authoritative (advisory) interpretation of Missouri law
(doctrine).

## Source

- **Index:** https://ago.mo.gov/other-resources/ag-opinions/
- **Publisher:** Missouri Attorney General's Office
- **Coverage:** 1933–present (~3,000 opinion PDFs)
- **Type:** doctrine (state legal opinions)

## How it works

The opinions are published as a nested set of WordPress pages:

```
/other-resources/ag-opinions/                              (index)
/other-resources/ag-opinions/2020-opinions/                (decade landing)
/other-resources/ag-opinions/2020-opinions/2024-opinions/  (year page)
```

The scraper does a breadth-first crawl of every `*-opinions/` page under
the AG-opinions tree, collecting the opinion PDF links on the
`ago.mo.gov/wp-content/uploads` CDN, then downloads each PDF and extracts
its text with the shared `common.pdf_extract.extract_pdf_markdown` helper
(OOM-hardened). PDFs are grouped by year and emitted newest-first.

Recent opinions are digitally-produced text PDFs (real text layer, no OCR
needed). A minority of older filings are scanned images; those yield no
extractable text and are skipped.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction test
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all years)
```

## License

[Public Domain (US Government Work — Missouri)](https://www.law.cornell.edu/uscode/text/17/105) — Missouri Attorney General opinions are official state government works in the public domain. No attribution required; commercial use permitted.
