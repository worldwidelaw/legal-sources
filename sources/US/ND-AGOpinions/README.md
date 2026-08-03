# US/ND-AGOpinions — North Dakota Attorney General Opinions

Full text of opinions issued by the **North Dakota Attorney General** —
Legal opinions, Advisory opinions, and Open Records & Meetings opinions.
Each opinion answers a legal question posed by a public official and
constitutes an authoritative interpretation of North Dakota law
(**doctrine**).

## Source

- **Publisher:** North Dakota Office of Attorney General
- **Index:** https://attorneygeneral.nd.gov/opinions-search/
- **Coverage:** 1942–present (~6,800 opinions)
- **Format:** Text-layer PDFs (real text, no OCR needed)

## How it works

The opinions index is a single server-rendered table (Ninja Table) at
`/opinions-search/`. One GET returns every opinion row — date issued,
requestor (issued-to), opinion type, and opinion number — each with a
direct link to the opinion PDF. There is no pagination, no JavaScript
rendering, and no CAPTCHA.

The scraper:
1. Fetches the index table page once.
2. Parses every `<tr data-row_id>` row into
   `(opinion_number, kind, issued_to, date_issued, pdf_url)`.
3. Downloads each PDF and extracts its text via `common.pdf_extract`.
4. Normalizes into the standard doctrine schema.

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## Note on rate limiting

`attorneygeneral.nd.gov` (WordPress) throttles bursts of rapid requests
at the connection level (refused TCP connections from the offending IP
for a cooldown period). The scraper paces requests at ~1 req/s; if run
from a datacenter IP it may need a residential/region proxy or a longer
inter-request delay.

## License

[Public Domain (US Government Work — North Dakota)](https://www.law.cornell.edu/uscode/text/17/105) — North Dakota Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
