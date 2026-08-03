# US/PA-AGOpinions — Pennsylvania Attorney General Official Opinions

Full text of the official opinions of the **Pennsylvania Attorney General** —
authoritative (advisory) interpretations of Pennsylvania law issued by the
Commonwealth's chief legal officer. This is a **doctrine** corpus.

## Coverage

Two groups of documents:

1. **Biennial bound volumes, 1895–1992** — each PDF is the full *Opinions of the
   Attorney General of Pennsylvania* report for a two-year (occasionally one- or
   multi-year) span, containing every formal opinion of that period.
2. **Individual modern opinions, 1994–2019** — standalone PDFs (Preate, Fisher,
   Corbett, Kelly, Kane and later).

The mid-century biennial volumes carry clean born-digital text layers
(126K–690K characters each). The rare scanned or empty PDF — including several of
the modern standalone opinions — is automatically skipped (no OCR is attempted).

## Access

The opinions are published as PDFs in the WordPress media store at
`https://www.attorneygeneral.gov/wp-content/uploads/...`. The HTML index page
(`/resources/official-ag-opinions/`) is JS/WAF-fronted and the WordPress REST API
is disabled, so the canonical list of 63 direct PDF URLs is embedded in
`bootstrap.py` (each harvested from the official page and verified HTTP 200,
`application/pdf`). The direct PDF endpoints themselves are **not** blocked.

Full text is extracted with the shared, OOM-hardened
`common.pdf_extract.extract_pdf_markdown` helper.

## Usage

```bash
python3 bootstrap.py bootstrap --sample   # ~12 sample documents
python3 bootstrap.py bootstrap            # full pull
python3 bootstrap.py test-api             # connectivity / extraction test
```

## License

[Public Domain (US Government Work — Pennsylvania)](https://www.law.cornell.edu/uscode/text/17/105) — Pennsylvania Attorney General opinions are official Commonwealth of Pennsylvania government works in the public domain. Commercial use permitted; no attribution required.
