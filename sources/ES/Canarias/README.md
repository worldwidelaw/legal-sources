# ES/Canarias — Canary Islands Regional Legislation (BOC)

Fetches legislation from the Boletín Oficial de Canarias (BOC), the official
gazette of the Canary Islands autonomous community.

## Data Source

- **URL**: https://www.gobiernodecanarias.org/boc/
- **Coverage**: 1980–present
- **Language**: Spanish (es)
- **Section**: I. Disposiciones Generales (core legislation)
- **Document types**: Leyes, Decretos, Órdenes, Resoluciones

## Strategy

1. Crawl yearly index pages to discover bulletin numbers and dates.
2. For each bulletin, parse the index page to identify Section I documents.
3. Fetch each document's HTML page and extract:
   - Metadata from META tags (title, date, entity, document type)
   - Full text from `<p class="justificado">` body elements
4. CVE identifiers (e.g., `BOC-A-2025-002-19`) serve as unique document IDs.

## Usage

```bash
python bootstrap.py bootstrap          # Full initial pull
python bootstrap.py bootstrap --sample # Fetch 10+ sample records
python bootstrap.py update             # Incremental update (last 30 days)
python bootstrap.py test               # Quick connectivity test
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Gobierno de Canarias open data.
