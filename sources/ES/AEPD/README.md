# AEPD - Agencia Española de Protección de Datos

Spanish Data Protection Authority resolutions fetcher.

## Data Source

- **URL**: https://www.aepd.es/informes-y-resoluciones/resoluciones
- **Format**: RSS feed with PDF documents
- **Coverage**: 2020-present (structured digital archive)
- **Update frequency**: Daily

## Resolution Types

| Code | Description |
|------|-------------|
| PS | Procedimiento Sancionador (sanction procedures) |
| AI | Archivo de Actuaciones (archived actions) |
| PD | Procedimiento de Derechos (rights procedures) |
| PA | Procedimiento de Apercibimiento (warning procedures) |
| TD | Tutela de Derechos (rights protection) |
| REPOSICION | Recurso de Reposición (appeals) |

## Usage

```bash
# Generate sample data (20 documents)
python3 bootstrap.py bootstrap --sample

# Generate more samples
python3 bootstrap.py bootstrap --sample --count 50

# Full bootstrap (streams all available documents)
python3 bootstrap.py bootstrap

# Fetch with enumeration (finds historical documents beyond RSS)
python3 bootstrap.py bootstrap --enumerate

# Fetch updates since a date
python3 bootstrap.py updates --since 2025-01-01
```

## Data Schema

Each normalized record contains:

| Field | Description |
|-------|-------------|
| _id | Unique document ID (e.g., "ps-00116-2025") |
| _source | Source identifier ("ES/AEPD") |
| _type | Document type ("case_law") |
| _fetched_at | ISO 8601 timestamp |
| title | Full resolution title |
| text | Full text extracted from PDF |
| date | Resolution date (YYYY-MM-DD) |
| url | Link to original PDF |
| expediente | Case file number (e.g., "EXP202503414") |
| resolution_type | Type code (PS, AI, PD, etc.) |
| resolution_number | Sequential number |
| resolution_year | Year of resolution |
| language | "es" |

## Technical Notes

- Full text is extracted from PDFs using pdfminer
- RSS feed provides the most recent ~100 resolutions
- Enumeration mode can discover historical documents by testing URL patterns
- Rate limiting: 1.5 seconds between PDF downloads

## License

Open government data under [Spanish Reuse of Public Sector Information regulations](https://datos.gob.es/en/terms).
