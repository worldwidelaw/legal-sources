# PA/Legispan — Panama National Assembly Legislation

Official Panama legislation from the Legispan platform operated by the National Assembly (Asamblea Nacional).

## Coverage

- **57,000+** norms from 1903 to present
- Types: Laws (LEY), Executive Decrees (DECRETO EJECUTIVO), Resolutions, Agreements, and 100+ other norm types
- Full text available for recent legislation (digitized gazette content)
- Spanish language

## Data Access

Public REST API at `https://legispan.asamblea.gob.pa/api/search/norm`:
- No authentication required
- Paginated results (max 100 per page)
- Full text included in JSON response (`original.content`)
- Filterable by type, authority, date range, keywords

## Usage

```bash
python bootstrap.py test-api             # Test API connectivity
python bootstrap.py bootstrap --sample   # Fetch 15 sample records
python bootstrap.py bootstrap            # Full fetch (all norms with text)
```

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (UUID from Legispan) |
| `title` | Full title of the norm |
| `text` | Full text content |
| `date` | Publication date (ISO 8601) |
| `norm_type` | Type (LEY, DECRETO EJECUTIVO, RESOLUCION, etc.) |
| `authority` | Issuing authority |
| `keywords` | Subject keywords |

## License

[Open Government Data](https://legispan.asamblea.gob.pa) — official legislation published by the National Assembly of Panama.
