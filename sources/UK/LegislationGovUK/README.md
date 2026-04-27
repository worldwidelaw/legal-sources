# UK/LegislationGovUK — UK National Archives Legislation Portal

## Overview

Official UK Government legislation database maintained by The National Archives.
Contains all UK Public General Acts from 1801-present, UK Statutory Instruments,
and devolved legislation for Wales, Scotland, and Northern Ireland.

## Data Source

- **URL:** https://www.legislation.gov.uk
- **API Documentation:** https://www.legislation.gov.uk/developer
- **License:** Open Government Licence v3.0
- **Language:** English

## Coverage

| Type | Name | Count |
|------|------|-------|
| ukpga | UK Public General Acts | 12,000+ |
| uksi | UK Statutory Instruments | 72,000+ |
| asp | Acts of Scottish Parliament | 600+ |
| wsi | Welsh Statutory Instruments | 3,000+ |
| ssi | Scottish Statutory Instruments | 10,000+ |

## API Access

The legislation.gov.uk API provides:

1. **ATOM Feed** for document discovery and pagination
   - `/{type}/data.feed` - List all documents of a type
   - `/{type}/data.feed?page=N` - Paginated access
   - Ordered by most recent first

2. **XML Full Text** for each document
   - `/{type}/{year}/{number}/data.xml` - Latest version
   - `/{type}/{year}/{number}/{date}/data.xml` - Point-in-time version
   - Full structured text with Akoma Ntoso-compatible markup

3. **Multiple formats available:**
   - XML (structured text)
   - HTML (web display)
   - PDF (print format)
   - RDF/XML (linked data)

## Full Text Extraction

Full text is extracted from the XML format, which contains:
- Long titles and enacting clauses
- Section headings and numbers
- Full body text of all provisions
- Schedules and appendices
- Explanatory notes (where available)

Average document size: 50-200KB of text per Act.

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (12,000+ records)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — free reuse with attribution.

## Notes

- No authentication required
- Rate limit: 2 requests/second recommended
- Consolidated versions show current law (amendments incorporated)
- Historical point-in-time versions available for most documents
- Excellent data quality — one of the best legal open data systems globally
