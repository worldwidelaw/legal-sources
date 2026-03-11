# EU/EuroParl - European Parliament Adopted Texts

## Overview

This source fetches adopted texts from the European Parliament, including:
- Legislative resolutions
- Non-legislative resolutions
- Legislative acts (co-decision procedure)
- Opinions, declarations, decisions
- Recommendations

## Data Access

### API Endpoint
- **List API**: `https://data.europarl.europa.eu/api/v2/adopted-texts`
- **Format**: JSON-LD (with `Accept: application/ld+json` header)
- **Pagination**: Offset-based (`?limit=100&offset=0`)

### Full Text
- **DOCEO Pages**: `https://www.europarl.europa.eu/doceo/document/{TA-ID}_EN.html`
- **Format**: HTML (parsed to extract text content)

### RSS Feed
- **URL**: `https://www.europarl.europa.eu/rss/doc/texts-adopted/en.xml`
- **Updates**: Recent adopted texts

## Coverage

- **Estimated Documents**: ~5,200+ adopted texts
- **Date Range**: 2019-present (EP 9th & 10th term)
- **Languages**: Multiple (EN primary)

## Usage

```bash
# Fetch sample records (12 documents)
python3 bootstrap.py bootstrap --sample

# Validate samples
python3 bootstrap.py validate

# Fetch with custom limit
python3 bootstrap.py bootstrap --limit 50
```

## Schema

| Field | Description |
|-------|-------------|
| `_id` | DOCEO document ID (e.g., TA-9-2020-0335) |
| `eli_id` | European Legislation Identifier |
| `title` | Document title (English) |
| `date` | Adoption date |
| `text` | Full text content |
| `parliamentary_term` | EP term (9th, 10th) |
| `eurovoc_concepts` | EuroVoc subject classifications |

## License

Open Data - European Parliament (CC BY 4.0)
