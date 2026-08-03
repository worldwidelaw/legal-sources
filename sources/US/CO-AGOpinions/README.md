# US/CO-AGOpinions — Colorado Attorney General Formal Opinions

Full text of formal legal opinions issued by the Colorado Department of Law
(Office of the Attorney General). Each opinion answers a legal question posed
by a public official and is an authoritative (advisory) interpretation of
Colorado law — classified as **doctrine**.

## Source

- **Index:** https://coag.gov/attorney-general-opinions/ (one WordPress page per year)
- **Documents:** digitally-produced text PDFs on `coag.gov/app/uploads/...`
- **Coverage:** 1994-present
- **Auth:** none

## How it works

1. Discover the per-year pages via the WordPress REST API
   (`/wp-json/wp/v2/pages?search=formal ag opinions`). The year slug varies
   (plural/singular/`-2` suffix), so the API search is more reliable than
   guessing slugs.
2. Fetch each year page and extract the `coag.gov/app/uploads/*.pdf` opinion
   links from its HTML.
3. Download each PDF and extract its text via the shared
   `common.pdf_extract.extract_pdf_markdown` helper (OOM-hardened). The PDFs
   carry a real text layer — no OCR needed.
4. Normalize into the standard doctrine schema (`text` holds the full opinion).

> Note: `coag.gov` requires TLS 1.3, which the system OpenSSL/LibreSSL on some
> hosts does not negotiate. The bootstrap falls back to a `curl` subprocess for
> those requests.

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all years)
```

## License

[Public Domain (US Government Work — Colorado)](https://www.law.cornell.edu/uscode/text/17/105) — Colorado Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
