# DK/Lovdata - Danish Legislation Database (Retsinformation)

Official Danish legislation from Retsinformation (retsinformation.dk).

## Data Source

- **Name**: Retsinformation
- **URL**: https://www.retsinformation.dk
- **Operator**: Civilstyrelsen (Danish Civil Affairs Agency)
- **License**: Open Data

## Coverage

### Document Types
- **LOV**: Laws (Lov)
- **LBK**: Consolidated laws (Lovbekendtgørelse)
- **BEK**: Executive orders (Bekendtgørelse)
- **CIR**: Circulars (Cirkulære)
- **VEJ**: Guidelines (Vejledning)
- **SKR**: Written statements (Skrivelse)

### Temporal Coverage
- Historical legislation from ~1985 to present
- Full text available for all documents

## API Access

### ELI XML Endpoint (Primary)
```
https://www.retsinformation.dk/eli/lta/{year}/{number}/xml
```
- No authentication required
- No rate limits observed
- Returns structured XML with full legal text

### Harvest API (Updates)
```
https://api.retsinformation.dk/v1/Documents?date=YYYY-MM-DD
```
- Available 03:00-23:45 CET only
- Returns documents changed within last 10 days
- Rate limit: 1 request per 10 seconds

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all years)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Data Schema

| Field | Description |
|-------|-------------|
| `_id` | Accession number (e.g., "B20240000105") |
| `title` | Document title in Danish |
| `text` | Full text of the legal document |
| `date` | Signature or publication date |
| `url` | ELI URI to original document |
| `document_type` | LOV, LBK, BEK, CIR, VEJ, etc. |
| `year` | Year of the document |
| `number` | Sequential number within year |
| `ministry` | Responsible ministry |
| `status` | Valid, Historic, etc. |

## ELI Implementation

Denmark has full ELI (European Legislation Identifier) implementation:
- Pillar I: URI structure (`/eli/lta/{year}/{number}`)
- Pillar II: Metadata schema
- Pillar III: RDFa microdata in HTML pages

## Notes

- XML format provides cleanest text extraction
- Documents are numbered sequentially within each year
- Some document numbers may be skipped (repealed or never published)

## License

[Open Data](https://www.retsinformation.dk) — Danish legislation is freely available via Retsinformation.
