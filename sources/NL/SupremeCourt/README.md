# NL/SupremeCourt - Dutch Supreme Court Case Law

## Overview

Fetches case law decisions from the Dutch Supreme Court (Hoge Raad der Nederlanden) using the Open Data API provided by de Rechtspraak (Council for the Judiciary).

## Data Source

- **Portal**: https://www.rechtspraak.nl
- **Open Data**: https://www.rechtspraak.nl/Uitspraken/Paginas/Open-Data.aspx
- **API Base**: https://data.rechtspraak.nl/uitspraken

## Coverage

- **Content**: Supreme Court (Hoge Raad) decisions
- **Volume**: 49,000+ decisions available
- **Period**: 1999 - present
- **Updates**: Daily
- **Language**: Dutch

## API Details

### Search Endpoint
```
GET https://data.rechtspraak.nl/uitspraken/zoeken
```

Parameters:
- `creator`: Court identifier (use `http://standaarden.overheid.nl/owms/terms/Hoge_Raad_der_Nederlanden` for Supreme Court)
- `max`: Maximum results per page
- `from`: Offset for pagination
- `date`: Date range filter (provide twice for range)
- `sort`: Sort order (DESC = newest first)

Returns: Atom feed with ECLI identifiers

### Content Endpoint
```
GET https://data.rechtspraak.nl/uitspraken/content?id={ECLI}
```

Returns: Full XML document with:
- RDF metadata (court, date, case number, subject area, procedure type, related cases)
- `<inhoudsindicatie>` - Abstract/summary
- `<uitspraak>` - Full decision text

## Authentication

No authentication required. Public open data access with rate limit of 10 requests/second.

## ECLI Format

European Case Law Identifier format: `ECLI:NL:HR:YYYY:NNNN`
- `NL` = Netherlands
- `HR` = Hoge Raad (Supreme Court)
- `YYYY` = Decision year
- `NNNN` = Sequential number

## Data Schema

```yaml
_id: ECLI identifier
_source: NL/SupremeCourt
_type: case_law
_fetched_at: ISO 8601 timestamp
title: Decision title
text: Full decision text (MANDATORY)
date: Decision date (YYYY-MM-DD)
url: Link to rechtspraak.nl detail page
ecli: ECLI identifier
case_number: Court case number
court: Hoge Raad
subject_area: Legal domain (Strafrecht, Civiel recht, Belastingrecht, etc.)
procedure_type: Procedure type (Cassatie, etc.)
summary: Decision abstract
related_cases: List of related ECLI identifiers
```

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (12+)
python bootstrap.py bootstrap --sample

# Full bootstrap (49K+ decisions)
python bootstrap.py bootstrap

# Incremental update (last 7 days)
python bootstrap.py update

# Update from specific date
python bootstrap.py update --since 2024-01-01
```

## License

Open Government Data. Free for commercial and non-commercial use.

## Notes

- All decisions are pseudonymized (personal data replaced with [betrokkene], etc.)
- Full text is in Dutch; no official translations available
- Related to NL/wetten.overheid.nl for legislation references
