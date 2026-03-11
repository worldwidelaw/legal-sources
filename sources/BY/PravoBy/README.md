# BY/PravoBy -- Belarus National Legal Portal (Codes)

Fetches all 26 codified laws (codes) from pravo.by, the official Belarus National Legal Internet Portal.

## Data Source

- **URL**: https://pravo.by
- **Country**: Belarus (BY)
- **Data Types**: Legislation (codified laws / codes)
- **Auth**: None (Open Government Data)

## Coverage

All 26 Belarusian codes including:
- Civil Code (Гражданский кодекс)
- Criminal Code (Уголовный кодекс)
- Labor Code (Трудовой кодекс)
- Tax Code (Налоговый кодекс)
- Administrative Code (Кодекс об административных правонарушениях)
- And 21 more specialized codes

## Strategy

1. Uses a curated list of known code registration numbers (regnum)
2. Fetches full text from `/document/?guid=3871&p0={regnum}`
3. Parses HTML to extract clean text content
4. All codes are consolidated (current) versions

## Usage

```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap
```

## Notes

- The codes are available in Russian
- Full text is provided via HTML rendering (no API)
- Rate limited to 0.5 requests/second to be respectful
- Related to GitHub issue #192

## Schema

Key fields:
- `regnum`: Registration number (e.g., "HK9800218")
- `title`: Code name in Russian
- `text`: Full text content
- `date`: Publication/enactment date
- `url`: Direct link to source
