# FI/SupremeAdministrativeCourt

Finnish Supreme Administrative Court (Korkein hallinto-oikeus / KHO) case law.

## Data Sources

The scraper runs two lanes and merges them on ECLI.

**1. Finlex — primary, live**
- Site: https://www.finlex.fi/fi/oikeuskaytanto/korkein-hallinto-oikeus
- Official case law service of the Finnish Ministry of Justice
- Collections crawled:
  | Collection | Contents | Years |
  |---|---|---|
  | `ennakkopaatokset` | Precedents / yearbook decisions | 1944– |
  | `lyhyet-ratkaisuselosteet` | Short case summaries | 1946–2021 |
  | `muut-paatokset` | Other published decisions | 2013– |

**2. LawSampo — secondary, frozen**
- SPARQL endpoint: http://ldf.fi/lawsampo/sparql
- 10,114 KHO judgments, `MAX(dateIssued) = 2021-08-04`
- Retained for its clean pre-2021 full text only; it can never yield newer decisions

## Data Access Method

Why the primary lane scrapes HTML rather than an API:

- The **Finlex open data REST API** (`https://opendata.finlex.fi/finlex/avoindata/v1`)
  documents `/akn/fi/judgment/{type}/list` endpoints, but they are **not deployed**.
  An unknown *act* or *doc* type returns `[] 200`, while *every* *judgment* type
  returns a bare `404` — case law is not yet served.
- The **bulk download page** offers no case-law archive (only "legal literature
  references").
- **LawSampo SPARQL** is the structured route, but it is frozen at 2021-08-04.

So the official Finlex website is the only source of post-2021 KHO full text.

finlex.fi is a Next.js application: the visible DOM is only an app shell, and the
decision body is delivered in the RSC (React Server Components) payload as a chain
of lazily-referenced rows. `finlex_web.py` rebuilds that payload, resolves the
`documentViews.fin` reference chain, and walks **only that subtree** — concatenating
every RSC text block would yield the app shell (navigation + schema.org blob)
rather than the judgment.

## Key Fields

- `ecli`: European Case Law Identifier (e.g. `ECLI:FI:KHO:2026:51`)
- `judgment_number`: decision label (e.g. `KHO:2026:51`, `KHO 13.8.2026/2047`)
- `diary_number`: diaarinumero (e.g. `220/2025`)
- `archival_record`: taltionumero
- `collection`: which Finlex collection the decision came from
- `keywords`: asiasanat
- `date`: decision date (antopäivä), ISO 8601
- `text`: full judgment text in Finnish
- `provenance`: `finlex.fi` or `lawsampo`

Pre-ECLI yearbook decisions do not render an ECLI on the page; the scraper
synthesises `ECLI:FI:KHO:{year}:{number}` for them, which is the identifier
LawSampo assigns to the same decisions, so the two lanes dedupe correctly.

## Incremental Updates

`fetch_updates(since)` walks the Finlex collections newest-year-first and is
limited to years `>= since.year`. LawSampo is excluded from the refresh lane
entirely — including it would re-crawl 10K frozen rows and write nothing, which
is exactly the failure reported in issue #1503.

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Finlex / Semantic
Finlex material is published under CC BY 4.0; attribution required, commercial
use permitted.

## Notes

- Latest decision confirmed retrievable: `ECLI:FI:KHO:2026:62` (2026-08-25)
- Sample records average ~17K characters of full decision text
