# IE/Acts — Irish Statute Book

## Overview

This data source fetches Irish legislation from the electronic Irish Statute Book (eISB), maintained by the Office of the Attorney General.

- **Website:** https://www.irishstatutebook.ie
- **Data Types:** Legislation (Acts and Statutory Instruments)
- **Authentication:** None required (open data)
- **Coverage:** 1922 to present
- **Documents:** ~3,500 Acts, ~35,000 Statutory Instruments, ~1,000 pre-1922 Acts

## ELI Implementation

Ireland implemented the European Legislation Identifier (ELI) system in September 2015. All legislation is accessible via ELI HTTP URIs:

- Acts: `/eli/{year}/act/{number}/enacted/en/html`
- Statutory Instruments: `/eli/{year}/si/{number}/made/en/html`
- Year listings: `/eli/{year}/act/` and `/eli/{year}/si/`

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample data (10+ records)
python bootstrap.py bootstrap --sample

# Full bootstrap (all documents)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Sample Output

Each normalized record contains:

```json
{
  "_id": "act/2024/1",
  "_source": "IE/Acts",
  "_type": "legislation",
  "_fetched_at": "2026-02-09T...",
  "title": "Policing, Security and Community Safety Act 2024",
  "text": "An Act to make further and better provision...",
  "date": "2024-02-07",
  "url": "https://www.irishstatutebook.ie/eli/2024/act/1/enacted/en/html",
  "doc_type": "act",
  "year": 2024,
  "number": 1,
  "language": "en"
}
```

## Rate Limiting

Conservative rate limiting at 2 requests/second with burst of 3.

## License

Irish legislation is Crown Copyright but available for reuse under open government license terms.

## References

- [ELI Register - Ireland](https://eur-lex.europa.eu/eli-register/ireland.html)
- [Irish ELI URI Schema (PDF)](https://www.irishstatutebook.ie/pdf/ELI_URI_schema.pdf)
- [N-Lex Ireland](https://n-lex.europa.eu/n-lex/info/info-ie/index)
