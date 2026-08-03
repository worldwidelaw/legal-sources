# US/NC-AGOpinions — North Carolina Attorney General Opinions

Full text of official legal opinions (advisory opinions and opinion letters)
issued by the **North Carolina Department of Justice / Office of the Attorney
General**. Each opinion answers a legal question posed by a public official or
state agency and constitutes an authoritative (advisory) interpretation of
North Carolina law — classified as **doctrine**.

## Source

- **Archive:** https://ncdoj.gov/legal-services/archived-opinions/
- **API:** WordPress REST API, custom post type `opinions`
  - `https://ncdoj.gov/wp-json/wp/v2/opinions?per_page=100&page=N`
- **Volume:** ~1,135 opinions (X-WP-Total), back to the mid-20th century.
- **Format:** Full text in `content.rendered` (HTML, stripped to clean text).
  No PDF download or OCR required.

## Access pattern

`ncdoj.gov` is a WordPress site. Opinions are a custom post type exposed via
the public WP REST API. The scraper pages through
`/wp-json/wp/v2/opinions?per_page=100&page=N` (newest first) and extracts:

- `title.rendered` → title
- `content.rendered` → full text (HTML tags/entities stripped)
- `date` → publication date (ISO 8601)
- `link` → canonical URL
- `id` → unique opinion identifier

The per-opinion front-end pages (`ncdoj.gov/opinions/{slug}/`) intermittently
return HTTP 500, so the scraper relies on the REST endpoint, which serves the
full text reliably. Rows with no usable full-text body are skipped.

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
python bootstrap.py bootstrap            # Full pull (all opinions)
```

## License

[Public Domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) — North Carolina Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
