# US/HI-AGOpinions — Hawaii Attorney General Opinions

Full text of official legal opinions issued by the **Hawaii Department of the
Attorney General**. Each opinion answers a legal question posed by a public
official or state agency and constitutes an authoritative (advisory)
interpretation of Hawaii law — classified as **doctrine**.

## Source

- **Opinions home:** https://ag.hawaii.gov/publications/ag-opinions/
- **Decade pages:** `past-ag-opinions/`, `1993-1999-ag-opinions/`,
  `2000-2009-ag-opinions/`, `2010-2019-ag-opinions/`
- **Volume:** ~67 opinion PDFs (back to the 1990s; older ones referenced in the
  master index).
- **Format:** Text-layer PDFs under `ag.hawaii.gov/wp-content/uploads/...`.
  ~44 carry a usable text layer locally; the rest are scanned images and are
  recovered via OCR on the VPS extraction backends.

## Access pattern

`ag.hawaii.gov` is a WordPress site that links every opinion as a direct PDF.
The scraper:

1. Fetches the landing pages and collects unique `wp-content/uploads/*.pdf`
   links, filtering out administrative PDFs (e.g. the annual org chart).
2. Downloads each PDF and extracts its text layer via
   `common.pdf_extract.extract_pdf_markdown`.
3. Derives the opinion number from the `NN-NN` / `YYYY-NN` filename token and
   parses the issue date from the opinion body.

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
python bootstrap.py bootstrap            # Full pull (all opinions)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — Hawaii Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
