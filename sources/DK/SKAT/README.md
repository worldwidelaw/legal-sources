# DK/SKAT - Danish Tax Authority Rulings

Fetches Danish tax rulings and binding answers (bindende svar) from Skatterådet and
Skattestyrelsen, published on Retsinformation.

## Data Source

- **Source**: Retsinformation (Danish Legal Information System)
- **URL**: https://www.retsinformation.dk
- **Ministry**: Skatteministeriet (Ministry of Taxation)
- **Authority**: Skatterådet (Tax Council)

## Document Types

- **AFG** (Afgørelser): Tax rulings and decisions
  - Binding rulings (bindende svar)
  - Tax decisions from Skatterådet

## API

Uses ELI (European Legislation Identifier) XML endpoints:

```
https://www.retsinformation.dk/eli/retsinfo/{year}/{number}/dan/xml
```

No authentication required - open data.

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (15 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Notes

- Tax rulings are scattered across the document number space
- Filters for documents from Skatteministeriet with AFG document type
- Full text is extracted from XML structure
- Rate limited to 1 request/second

## License

[Open Data](https://www.retsinformation.dk) — Danish tax rulings are freely available via Retsinformation.
