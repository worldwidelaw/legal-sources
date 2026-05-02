# ES/Aragon — Aragón Regional Legislation (BOA)

Fetches legislative dispositions from the **Boletín Oficial de Aragón (BOA)**, the
official gazette of the Autonomous Community of Aragón, Spain.

## Data Source

- **URL**: https://www.boa.aragon.es
- **Coverage**: 90,000+ legislative dispositions since 1978
- **Data types**: Legislation (Leyes, Decretos, Órdenes, Resoluciones, Acuerdos, Convenios)
- **Language**: Spanish (es)
- **Update frequency**: Daily

## Method

Uses the BOA BRSCGI CGI system:
1. `CMD=VERLST` to paginate through document listings (section: DISPOSICIONES)
2. `CMD=VERDOC&DOCN={id}` to fetch full text of each document
3. HTML parsing extracts titles, dates, sections, issuers, and full body text

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Attribution required.
