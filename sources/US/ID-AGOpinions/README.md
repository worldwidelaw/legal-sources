# US/ID-AGOpinions — Idaho Attorney General Opinions

Full text of formal opinions issued by the **Idaho Attorney General**.
Each opinion answers a legal question posed by a public official
(legislator, state agency, or official) and constitutes an authoritative
interpretation of Idaho law (**doctrine**).

## Source

- **Publisher:** Idaho Office of the Attorney General
- **Listing:** https://www.ag.idaho.gov/office-resources/opinions/
- **Coverage:** ~2005–present (~25 formal opinions)
- **Format:** Text-layer PDFs (real text, no OCR needed)

## How it works

The public opinions listing (`/office-resources/opinions/`) is rendered
by JavaScript, so it exposes no PDF links in its static HTML. Instead the
scraper queries the site's **WordPress REST API** media endpoint:

```
GET /wp-json/wp/v2/media?search=opinion&per_page=100&page={N}
```

This returns clean JSON for every media item. The scraper:

1. Pages through the media API (`search=opinion` and
   `search=certificate of review`).
2. Keeps PDFs whose filename matches the formal opinion naming pattern
   (`Opinion-21-01.pdf`, `Opinion09-01.pdf`, …) plus the
   `Published-Opinion` series.
3. Derives the opinion number and issue year from the filename
   (e.g. `Opinion09-01` → number `09-1`, year 2009).
4. Downloads each PDF and extracts its text via `common.pdf_extract`.
5. Normalizes into the standard doctrine schema.

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
python bootstrap.py bootstrap            # Full pull (all opinions)
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
```

## Notes

- A handful of older opinions are scanned images that yield 0 chars when
  OCR (tesseract) is unavailable; these are skipped locally and recovered
  on hosts with OCR enabled.
- `www.ag.idaho.gov` intermittently presents an expired/flaky TLS
  certificate to python's `requests` (some PDF fetches fail transiently);
  the scraper paces requests at ~1 req/s and tolerates the skips.

## License

[Public Domain (US Government Work — Idaho)](https://www.law.cornell.edu/uscode/text/17/105) — Idaho Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
