# ES/Galicia — Galicia Regional Legislation (DOG)

Regional legislation from the Diario Oficial de Galicia (DOG), accessed via the
BOE (Boletín Oficial del Estado) ELI hierarchy and Open Data API.

## Data Source

- **Publisher:** Comunidad Autónoma de Galicia / Boletín Oficial del Estado
- **URL:** https://www.boe.es/eli/es-ga
- **Coverage:** 1982–present
- **Language:** Spanish (es)
- **Document types:** Leyes, Decretos, Decretos Legislativos, Órdenes, Resoluciones

## Methodology

1. Discover all Galicia documents from BOE ELI sitemaps (es-ga prefix)
2. Extract BOE-A identifier from each ELI document page
3. Fetch metadata via BOE Open Data API (JSON)
4. Fetch full text via BOE Open Data API (XML)

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — BOE open government data, attribution required.
