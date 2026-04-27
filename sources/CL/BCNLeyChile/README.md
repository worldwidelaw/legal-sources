# CL/BCNLeyChile — Biblioteca del Congreso Nacional - Ley Chile

Chile's official legislation database with 410,000+ norms in full text.

## Data Source

- **URL**: https://www.bcn.cl/leychile/
- **Linked Data**: https://datos.bcn.cl/es/
- **Type**: Legislation (laws, decrees, resolutions, DFL)
- **Auth**: None required
- **Language**: Spanish

## API Endpoints

Uses internal JSON API discovered from the Angular SPA:

- **Search**: `https://nuevo.leychile.cl/servicios/buscarjson`
  - Params: `string`, `tipoNorma`, `page_num`, `itemsporpagina`, `orden`
  - Returns: `[results[], metadata{}, facets{}]`
- **Full Text**: `https://nuevo.leychile.cl/servicios/Navegar/get_norma_json`
  - Params: `idNorma`, `tipoVersion=Vigente`, `agrupa_partes=1`
  - Returns: `{html[], metadatos{}, estructura{}, ...}`

## Usage

```bash
python bootstrap.py test-api             # Test connectivity
python bootstrap.py bootstrap --sample   # Fetch 15 sample records
python bootstrap.py bootstrap            # Full bootstrap (all norms)
```

## Notes

- User-Agent header required (site blocks default python-requests UA)
- Rate limit: 1 request/second for full text retrieval
- datos.bcn.cl SPARQL endpoint has metadata only, not full text
- Full text is returned as HTML sections; cleaned to plain text

## License

[Open Government Data](https://www.bcn.cl/leychile/) — official legislation published by the Biblioteca del Congreso Nacional de Chile.
