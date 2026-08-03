# US/FL-AGOpinions — Florida Attorney General Formal Opinions

Full text of formal legal opinions ("AGOs") issued by the Florida Office of
the Attorney General. Each opinion answers a legal question posed by a public
official and is an authoritative (advisory) interpretation of Florida law —
classified as **doctrine**.

## Source

- **Index:** https://www.myfloridalegal.com/ag-opinions (paginated, ~30 rows/page)
- **Detail pages:** `https://www.myfloridalegal.com/node/{id}` — full opinion text in HTML
- **Coverage:** ~5,000 opinions back to the 1970s
- **Auth:** none

## How it works

1. Walk the paginated index `/ag-opinions?page=N`. Each table row provides the
   opinion number (`AGO YYYY-NN`), the issued date (`<time datetime>`), the
   title, and the `/node/{id}` detail link.
2. Fetch each `/node/{id}` page and extract the full opinion body from the
   Drupal `field--name-body` region (HTML — no PDFs).
3. Normalize into the standard doctrine schema (`text` holds the full opinion).

## Usage

```bash
python bootstrap.py test-api            # connectivity + parse check
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all pages)
```

## License

[Public Domain (US Government Work — Florida)](https://www.law.cornell.edu/uscode/text/17/105) — Florida Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
