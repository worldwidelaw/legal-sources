# LT/Parliament — Lithuanian Parliament (Seimas) Publications

Official publications from the Lithuanian Parliament (Seimas) website, including press releases, meeting agendas, committee work plans, parliamentary statements, and other official documents.

## Data Source

- **Portal**: [data.gov.lt](https://data.gov.lt/datasets/2609/)
- **API**: `https://get.data.gov.lt/datasets/gov/lrsk/interneto_tekstai/Straipsnis`
- **Provider**: Lietuvos Respublikos Seimo kanceliarija (Parliament Chancellery)
- **License**: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Coverage

- **Time Range**: 1999 to present
- **Languages**: Lithuanian (LT), English (EN), German (DE), French (FR), Russian (RU)
- **Content Types**:
  - Press releases
  - Meeting agendas
  - Committee work plans
  - Parliamentary statements
  - Official announcements
  - Biographical information

## Relationship to LT/LegalBase

This source **complements** LT/LegalBase, not replaces it:

| Source | Coverage |
|--------|----------|
| **LT/LegalBase** | Enacted legislation from TAR (Register of Legal Acts) |
| **LT/Parliament** | Parliamentary publications and communications |

LT/LegalBase covers the final enacted laws, while LT/Parliament covers the parliamentary process, institutional communications, and supporting documents.

## Data Fields

| Field | Description |
|-------|-------------|
| `vda_id` | Unique document identifier |
| `title` | Document title |
| `text` | Full text content |
| `language` | Language code (LT/EN/DE/FR/RU) |
| `date` | Publication date |
| `url` | Link to source document |
| `correction_date` | Last modification date |

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap (all records)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Lithuanian open government data.

## API Notes

- Uses cursor-based pagination via `_page` parameter
- Full text directly available in `tekstas_lt` field
- Rate limit: 1 request/second
- Updates published monthly
