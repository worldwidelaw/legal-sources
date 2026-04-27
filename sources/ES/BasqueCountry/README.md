# ES/BasqueCountry — Basque Country Regional Legislation (BOPV/EHAA)

## Overview

This source fetches legislation from the **Basque Autonomous Community** (País Vasco / Euskadi) via the Open Data Euskadi platform. The data comes from the Official Gazette (Boletín Oficial del País Vasco / Euskal Herriko Agintaritzaren Aldizkaria).

## Data Source

- **Portal**: [Open Data Euskadi](https://opendata.euskadi.eus)
- **SPARQL Endpoint**: https://api.euskadi.eus/sparql/
- **License**: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)
- **Coverage**: 1936 to present
- **Languages**: Spanish (es) and Basque (eu)

## Data Access Method

1. **SPARQL Query**: Query the ELI (European Legislation Identifier) dataset for legislation metadata and XML content URLs
2. **XML Fetch**: Retrieve full text from the `legegunea.euskadi.eus` content server
3. **Parse**: Extract title, full text, and metadata from XML

## Document Types

| Code | Type (Spanish) | Type (English) |
|------|---------------|----------------|
| d    | Decreto       | Decree         |
| o    | Orden         | Order          |
| res  | Resolución    | Resolution     |
| l    | Ley           | Law            |
| ac   | Acuerdo       | Agreement      |
| df   | Decreto Foral | Provincial Decree |

## ELI URI Structure

Documents use European Legislation Identifier (ELI) URIs:
```
https://id.euskadi.eus/eli/es-pv/{type}/{year}/{month}/{day}/{number}/dof/{lang}
```

Example:
```
https://id.euskadi.eus/eli/es-pv/d/2020/09/29/198/dof/spa
```
- `es-pv`: Basque Country jurisdiction code
- `d`: Document type (decreto)
- `2020/09/29`: Date
- `198`: Document number
- `spa`: Spanish language version

## Output Schema

```json
{
  "_id": "es-pv_d_2020-09-29_198",
  "_source": "ES/BasqueCountry",
  "_type": "legislation",
  "_fetched_at": "2026-02-21T13:00:00+00:00",
  "title": "DECRETO 198/2020, de 29 de septiembre...",
  "text": "Full text of the legislation...",
  "date": "2020-09-29",
  "url": "https://id.euskadi.eus/eli/es-pv/d/2020/09/29/198/dof/spa",
  "eli_uri": "https://id.euskadi.eus/eli/es-pv/d/2020/09/29/198/dof/spa",
  "document_type": "decreto",
  "boletin_numero": "196",
  "entidad": "Gobierno Vasco",
  "departamentos": ["Seguridad"],
  "categorias": ["PERSONAL", "NOMBRAMIENTOS"],
  "language": "es",
  "jurisdiction": "es-pv"
}
```

## Usage

```bash
# Fetch sample data (12 records)
python3 bootstrap.py bootstrap --sample

# Fetch more records
python3 bootstrap.py bootstrap --sample --limit 50

# Validate sample data
python3 bootstrap.py validate
```

## Notes

- The SPARQL endpoint provides metadata; full text is fetched from XML endpoints
- Spanish language versions are fetched by default (more standardized)
- XML content includes HTML markup which is cleaned to plain text
- PDF versions are also available and tracked in metadata

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — attribution required.

## Related Sources

- **ES/BOE**: Spanish national legislation (Boletín Oficial del Estado)
- **ES/ConstitutionalCourt**: Spanish Constitutional Court decisions
