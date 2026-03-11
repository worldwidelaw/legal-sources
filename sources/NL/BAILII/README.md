# NL/BAILII - Dutch Case Law (Rechtspraak.nl)

Dutch court decisions from the Rechtspraak.nl Open Data service.

## Data Source

- **URL**: https://data.rechtspraak.nl
- **Documentation**: https://www.rechtspraak.nl/Uitspraken/Paginas/Open-Data.aspx
- **Technical Docs**: https://www.rechtspraak.nl/SiteCollectionDocuments/Technische-documentatie-Open-Data-van-de-Rechtspraak.pdf
- **License**: CC0 / Public Domain

## Coverage

- **Records**: 900,000+ court decisions
- **Courts**: All Dutch courts including:
  - Hoge Raad (Supreme Court)
  - Gerechtshoven (Courts of Appeal)
  - Rechtbanken (District Courts)
  - Centrale Raad van Beroep (Central Appeals Tribunal)
  - College van Beroep voor het bedrijfsleven (Trade and Industry Appeals Tribunal)
- **Date Range**: 1998 onwards (most complete from 2013)

## API

### Search Endpoint
```
GET https://data.rechtspraak.nl/uitspraken/zoeken
```

Parameters:
- `max`: Maximum results (default 10, max 1000)
- `date`: Filter by decision date (e.g., `>=2023-01-01`)
- `modified`: Filter by modification date
- `sort`: ASC or DESC
- `return`: DOC (full) or ECLI (IDs only)

### Content Endpoint
```
GET https://data.rechtspraak.nl/uitspraken/content?id={ECLI}
```

Returns full XML document with RDF metadata and decision text.

### Rate Limit
Maximum 10 requests per second.

## ECLI Format

European Case Law Identifier format:
```
ECLI:NL:{COURT}:{YEAR}:{NUMBER}
```

Examples:
- `ECLI:NL:HR:2023:123` - Hoge Raad (Supreme Court)
- `ECLI:NL:GHAMS:2023:456` - Gerechtshof Amsterdam (Court of Appeal)
- `ECLI:NL:RBAMS:2023:789` - Rechtbank Amsterdam (District Court)

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (caution: 900K+ records)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Schema

Key fields in normalized records:
- `_id`: ECLI identifier
- `ecli`: European Case Law Identifier
- `court`: Court name (Dutch)
- `case_number`: Internal case number
- `date`: Decision date
- `text`: Full text of the decision (MANDATORY)
- `legal_area`: Legal area (civil, criminal, administrative)
- `procedure`: Procedure type
- `url`: Deeplink to official page
