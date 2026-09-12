# DE/BaFin - German Federal Financial Supervisory Authority

## Overview

BaFin (Bundesanstalt für Finanzdienstleistungsaufsicht) is Germany's integrated financial regulatory authority, supervising banks, insurance companies, investment funds, and securities trading.

## Data Source

This fetcher retrieves BaFin's **Verwaltungspraxis** (administrative practice) corpus — the binding
guidance the authority issues to supervised institutions:

- **Rundschreiben** (circulars) — MaRisk, MaComp, AML high-risk country lists, fit-and-proper requirements
- **Auslegungsentscheidungen** (interpretative decisions)
- **Merkblätter** (guidance notices)
- **Aufsichtsmitteilungen** (supervisory notices)

## Technical Details

- **Method**: HTML scraping of the Servicesuche result pages and document pages
- **Documents**: ~430 across the four format facets
- **Search endpoint**: `https://www.bafin.de/SiteGlobals/Forms/Suche/Expertensuche/Servicesuche_Formular.html?cl2Categories_Format={facet}`
- **Document URLs**:
  - `https://www.bafin.de/SharedDocs/Veroeffentlichungen/DE/{Format}/{YEAR}/{slug}.html` — full text in `div.l-article`
  - `https://www.bafin.de/SharedDocs/Downloads/DE/{Format}/{slug}.html` — landing page whose PDF is extracted via `common/pdf_extract`
- **Rate Limit**: 1.5 seconds between requests

### Pagination gotcha (issue #1571)

BaFin migrated to a new site in 2026. The previous
`/SiteGlobals/Forms/Suche/Expertensuche_Formular.html` endpoint now returns 404 for every
client, and result items moved from `div.search-result` to `div.c-teaser-search-result`.

More subtly, the new search **accepts `pageNo` with HTTP 200 but always serves page 0** —
paging is driven by an opaque `gtp=<node>_list%3D<n>` token. The fetcher therefore follows
the rendered `c-pagination__button--next` link and asserts that each page contributes new
URLs, rather than incrementing a counter (which would silently cap the corpus at 50 docs).

## Usage

```bash
# Test mode (3 documents)
python3 bootstrap.py

# Full bootstrap (whole corpus -> data/records.jsonl)
python3 bootstrap.py bootstrap

# Sample bootstrap (15 documents)
python3 bootstrap.py bootstrap --sample
```

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Unique document identifier |
| `_source` | Always "DE/BaFin" |
| `_type` | Always "doctrine" |
| `title` | Circular title |
| `text` | Full text content |
| `date` | Publication date (ISO 8601) |
| `url` | Original document URL |
| `topic` | Document format (Rundschreiben / Auslegungsentscheidung / Merkblatt / Aufsichtsmitteilung) |
| `pdf_url` | Source PDF, when the body came from a Downloads landing page |
| `reference` | Reference number if available |
| `authority` | Always "BaFin" |
| `language` | Always "de" |

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).
