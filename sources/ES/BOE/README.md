# ES/BOE -- Boletín Oficial del Estado

Spanish Official State Gazette - Consolidated Legislation

## Overview

The BOE (Boletín Oficial del Estado) is Spain's official gazette, publishing all state
legislation, regulations, and official announcements. This source fetches consolidated
legislation through the official Open Data API.

## Data Access

**Official API**: https://www.boe.es/datosabiertos/

The BOE provides a well-documented REST API with XML/JSON responses:

| Endpoint | Description |
|----------|-------------|
| `/legislacion-consolidada` | List consolidated laws with pagination |
| `/legislacion-consolidada/id/{id}/metadatos` | Document metadata |
| `/legislacion-consolidada/id/{id}/texto` | Full text in structured XML |
| `/legislacion-consolidada/id/{id}/analisis` | Legal analysis and references |

## Document Types

- **Ley**: Laws passed by Parliament
- **Real Decreto**: Royal Decrees
- **Real Decreto-ley**: Emergency Royal Decrees
- **Orden**: Ministerial Orders
- **Resolución**: Resolutions

## ELI (European Legislation Identifier)

Spain implements ELI for legislation identification:

```
https://www.boe.es/eli/es/{type}/{year}/{month}/{day}/{number}
```

Example: https://www.boe.es/eli/es/l/1978/12/27/78 (Spanish Constitution)

## License

Open Data - Free reuse under Spanish open data regulations.

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (10+)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Notes

- API is public, no authentication required
- Rate limit: 2 requests/second recommended
- Full text is returned as structured XML with articles, paragraphs, etc.
- Consolidated versions include all amendments
