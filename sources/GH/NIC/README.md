# GH/NIC — Ghana National Insurance Commission

Ghana's National Insurance Commission (NIC) regulates the insurance industry
under the Insurance Act, 2021 (Act 1061). This source fetches regulatory
directives, guidelines, market reports, public notices and news.

## Data types

- **doctrine**: Regulatory directives and guidelines, statutory public notices,
  annual/quarterly market reports, market research, and news releases

## Strategy

The Commission retired its WordPress site at `nicgh.org` — that host now 301s
to `nic.gov.gh`, a Vite SPA whose content comes from a **public Strapi v5 API**
(no auth). The old `wp-json/wp/v2` endpoints are gone (see issue #1511), so the
scraper reads the Strapi collections directly:

| Collection | Count | Full text |
|---|---|---|
| `/api/regulatory-docs` | 33 | PDF (`file.url`), extracted with pdfplumber |
| `/api/publications` | 22 | PDF (`file.url`), extracted with pdfplumber |
| `/api/notices` | 5 | inline Strapi rich-text blocks (`body`) |
| `/api/news-articles` | 8 | inline Strapi rich-text blocks (`body`) |

Total: ~68 documents. PDFs are served from
`nicsws.blob.core.windows.net/strapi-uploads` (Azure blob, public read; the
container itself is not listable). Records with under 150 characters of
extracted text are skipped.

`fetch_updates(since)` filters each collection on `filters[updatedAt][$gte]`,
normalizing `since` through `common.base_scraper.as_date_str` so a `datetime`
from the update runner cannot leak into the query string.

## Usage

```bash
python bootstrap.py test               # counts per collection + one PDF extract
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap-fast     # full corpus
python bootstrap.py update             # incremental
```

## License

[Public Government Documents (Ghana)](https://nic.gov.gh/disclaimer) — official
regulatory publications of the National Insurance Commission. Attribution
required; commercial use permitted.
