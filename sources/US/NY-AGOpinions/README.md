# US/NY-AGOpinions — New York Attorney General Legal Opinions

Full text of formal and informal legal opinions issued by the New York State
Department of Law (Office of the Attorney General). Each opinion answers a legal
question posed by a public official and is an authoritative (advisory)
interpretation of New York law — classified as **doctrine**.

## Source

- **Index:** https://ag.ny.gov/libraries-documents/opinions (paginated HTML list)
- **Documents:** digitally-produced text PDFs on `ag.ny.gov/sites/default/files/opinions/...`
- **Coverage:** 1995-present
- **Auth:** none

## How it works

1. Walk the paginated index
   (`/libraries-documents/opinions/opinions-year?page=N`, ~22 pages, newest
   first), stopping after two consecutive empty pages.
2. Extract the `/sites/default/files/opinions/*.pdf` opinion links from each
   index page and resolve them against `https://ag.ny.gov`.
3. Download each PDF and extract its text via the shared
   `common.pdf_extract.extract_pdf_markdown` helper (OOM-hardened). The PDFs
   carry a real text layer — no OCR needed.
4. Normalize into the standard doctrine schema (`text` holds the full opinion).
   The filename slug encodes the opinion number and whether it is **formal**
   (`YYYY-F#`) or **informal** (`YYYY-#` / `I_YY-#`); both are captured.

> Note: `ag.ny.gov` requires TLS 1.3, which the system OpenSSL/LibreSSL on some
> hosts does not negotiate. The bootstrap falls back to a `curl` subprocess for
> those requests.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all pages)
```

## License

[Public Domain (US Government Work — New York)](https://www.law.cornell.edu/uscode/text/17/105) — New York Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
