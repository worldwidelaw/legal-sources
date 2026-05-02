# ES/Asturias — Asturias Regional Legislation (BOPA)

Fetches regional legislation from the Boletín Oficial del Principado de Asturias (BOPA).

## Data Source

- **Publisher:** Gobierno del Principado de Asturias
- **URL:** https://sede.asturias.es/bopa
- **Coverage:** 2019–present (DISPOSICIONES GENERALES section)
- **Estimated records:** ~100–160 per year
- **Language:** Spanish (es)
- **Data types:** legislation (Leyes, Decretos, Órdenes, Resoluciones)

## Access Method

1. **Discovery:** Annual JSON index files from the Asturias Open Data portal at `descargas.asturias.es`
2. **Full text:** Liferay detail endpoint at `miprincipado.asturias.es` (extracts from `<div id="bopa-articulo">`)
3. **Incremental:** Daily sumario endpoint for 2025+ updates

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Attribution to Administración del Principado de Asturias required.
