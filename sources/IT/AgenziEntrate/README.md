# IT/AgenziEntrate - Italian Revenue Agency Tax Doctrine

Italian Revenue Agency (Agenzia delle Entrate) tax doctrine and guidance documents.

## Data Types

- **Interpelli**: Tax rulings (responses to taxpayer questions) - ~300+/year
- **Circolari**: Circulars (official guidance) - ~15/year
- **Risoluzioni**: Resolutions (technical clarifications) - ~70+/year

## Source

- URL: https://www.agenziaentrate.gov.it
- Authority: Agenzia delle Entrate (Italian Revenue Agency)
- License: Open Government Data
- Language: Italian

## Collection Method

1. Scrape yearly and monthly index pages for each document type
2. Extract PDF URLs from document listings
3. Download PDFs and extract full text using pdfplumber
4. Normalize to standard schema

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Unique ID: `IT:AE:{type}:{number}_{year}` |
| `doc_type` | Type: `interpello`, `circolare`, `risoluzione` |
| `doc_number` | Document number |
| `year` | Publication year |
| `title` | Document title |
| `text` | Full text content |
| `date` | Publication date (ISO 8601) |
| `url` | PDF download URL |

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Coverage

- Interpelli: 2018-present
- Circolari: 2018-present
- Risoluzioni: 2018-present
