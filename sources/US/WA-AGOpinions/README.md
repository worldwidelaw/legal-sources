# US/WA-AGOpinions — Washington State Attorney General Opinions

Full text of formal opinions ("AGOs") issued by the Washington State Office of
the Attorney General. Each opinion answers a legal question posed by a public
official and is an authoritative (advisory) interpretation of Washington law —
classified as **doctrine**.

## Source

- **Year indexes:** https://www.atg.wa.gov/ago-opinions/year/{YYYY} (1949–present, ~10/year)
- **Detail pages:** `https://www.atg.wa.gov/ago-opinions/{slug}` — full opinion text in HTML
- **Auth:** none

## How it works

1. For each year (current → 1949) fetch `/ago-opinions/year/{YYYY}` and collect
   the opinion-slug detail links.
2. Fetch each `/ago-opinions/{slug}` page. The opinion body is rendered inline
   in the main `container no-sidebars` region (HTML — no PDFs). Pull the AGO
   number (parsed from text), issued date (`<time datetime>`), title (`<h1>`),
   and full text.
3. Normalize into the standard doctrine schema (`text` holds the full opinion).

## Usage

```bash
python bootstrap.py test-api            # connectivity + parse check
python bootstrap.py bootstrap --sample  # ~12 sample documents
python bootstrap.py bootstrap           # full pull (all years)
```

## License

[Public Domain (US Government Work — Washington)](https://www.law.cornell.edu/uscode/text/17/105) — Washington Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
