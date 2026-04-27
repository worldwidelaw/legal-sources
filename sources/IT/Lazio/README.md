# IT/Lazio — Lazio Regional Legislation

Regional laws (Leggi Regionali) from the Lazio region of Italy.

## Source

- **Website:** https://www.consiglio.regione.lazio.it/?vw=leggiregionali
- **Data Type:** Legislation
- **Coverage:** 1971–present
- **License:** CC BY-NC-SA 2.0

## Data Access Method

This source uses **HTML scraping** of the Consiglio Regionale del Lazio website:

1. **Search endpoint:** Queries by year with pagination (10 results per page)
2. **Detail pages:** Fetches full text from individual law pages

### Endpoints

- Search: `https://www.consiglio.regione.lazio.it/?vw=leggiregionali&sv=vigente&annoLegge={year}&pg={page}`
- Detail: `https://www.consiglio.regione.lazio.it/?vw=leggiregionalidettaglio&id={id}&sv=vigente`

## Data Content

Each record includes:

| Field | Description |
|-------|-------------|
| `title` | Law title |
| `text` | Full text of the law (HTML cleaned) |
| `date` | Publication date (ISO 8601) |
| `law_number` | Law number within year |
| `year` | Year of enactment |
| `bur_number` | BUR (Official Gazette) issue number |
| `bur_date` | BUR publication date |
| `url` | Link to coordinated text |

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (10+)
python bootstrap.py bootstrap --sample

# Full bootstrap (all years)
python bootstrap.py bootstrap

# Update recent years
python bootstrap.py update
```

## Estimated Volume

- ~20-30 laws per year
- ~1,500+ total regional laws since 1971
- Average text length: 5-15KB per law

## License

> ⚠️ **Commercial use prohibited.** This data is licensed under a non-commercial license.

[CC BY-NC-SA 2.0](https://creativecommons.org/licenses/by-nc-sa/2.0/) — non-commercial use only, attribution required, share-alike.

## Notes

- Uses "testo coordinato" (consolidated text with amendments)
- Rate limited to 30 requests/minute to respect server capacity
- BUR (Bollettino Ufficiale Regione) dates indicate official publication
