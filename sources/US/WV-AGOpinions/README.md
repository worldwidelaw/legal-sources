# US/WV-AGOpinions — West Virginia Attorney General Opinions

Full text of legal opinions issued by the **West Virginia Office of the
Attorney General**. Each opinion answers a legal question posed by a public
official (county commission, prosecuting attorney, state agency, etc.) and
constitutes an authoritative advisory interpretation of West Virginia law.
These are classified as **doctrine**.

## Source

- **Hub:** https://ago.wv.gov/attorney-general-opinions
- **Publisher:** West Virginia Office of the Attorney General
- **Coverage:** roughly 2000–present
- **Format:** digitally-produced text PDFs served via Drupal media download
  URLs (`/media/{id}/download`) and a few direct `/sites/default/files/*.pdf`
  links

## How it works

1. Fetch the hub page; collect the date-range archive page links it lists.
   The hub mixes two URL shapes: `/page/attorney-general-opinions-2024-2020`
   and `/attorney-general-opinions-2014-2010` (no `/page/` prefix).
2. Parse each index page for opinion anchors → (PDF URL, descriptive title).
   The anchor text is the opinion's subject with a trailing
   `(Month D, YYYY)` issued date.
3. Download each PDF and extract its text via the shared
   `common.pdf_extract.extract_pdf_markdown` helper (OOM-hardened).
4. Derive the issued date from the title, falling back to the opinion's
   opening lines; normalize into the standard doctrine schema.

A minority of older (pre-2010) opinions are scanned image PDFs with no text
layer; these yield 0 chars and are skipped.

## Usage

```bash
python bootstrap.py bootstrap            # Full pull (all index pages)
python bootstrap.py bootstrap --sample   # Fetch ~12 sample documents
python bootstrap.py bootstrap-fast       # Alias for full pull (VPS wrapper)
python bootstrap.py test-api             # Connectivity test
```

## License

[Public Domain (US Government Work — West Virginia)](https://www.law.cornell.edu/uscode/text/17/105) — West Virginia Attorney General opinions are official state government works in the public domain. Commercial use permitted; no attribution required.
