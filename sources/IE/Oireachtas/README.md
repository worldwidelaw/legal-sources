# IE/Oireachtas — Houses of the Oireachtas (Irish Parliament)

Irish parliamentary debates and proceedings.

## Data Source

- **API**: https://api.oireachtas.ie/v1
- **Data Files**: https://data.oireachtas.ie
- **Documentation**: https://api.oireachtas.ie (Swagger UI)

## Coverage

- **Date Range**: 1919 to present
- **Chambers**: Dáil Éireann (lower house), Seanad Éireann (upper house)
- **Content**: Full transcripts of debates, questions, bill discussions

## Data Format

Debates are available in Akoma Ntoso XML format, an international standard for
legislative documents. The XML includes:

- Speaker identification
- Speech content
- Questions and answers
- Timestamps
- References to bills and legislation

## License

Oireachtas (Open Data) PSI Licence — Creative Commons Attribution 4.0 equivalent.

## Usage

```bash
# Test API connectivity
python bootstrap.py test

# Fetch sample records (12 debates)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/v1/debates` | List debate records with metadata |
| `/v1/legislation` | Bills and acts (links to IE/Acts) |
| `/v1/questions` | Parliamentary questions |

## Notes

This source focuses on parliamentary proceedings (debates, questions). For
enacted legislation with full text, see `IE/Acts` which covers the Irish
Statute Book via ELI URIs.
