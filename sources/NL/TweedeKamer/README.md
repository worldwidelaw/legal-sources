# NL/TweedeKamer — Dutch House of Representatives

## Overview

Parliamentary proceedings from the Tweede Kamer der Staten-Generaal (Dutch House of Representatives).

**Data Type:** Parliamentary Proceedings (NOT legislation)
**Content:** Plenary debate transcripts, committee meetings, motions, amendments, parliamentary questions
**API:** OData v4 at gegevensmagazijn.tweedekamer.nl
**License:** [Public Domain](https://data.overheid.nl/licenties) (Dutch Open Government Data)
**Auth:** None required

## Important Note

This source contains **parliamentary proceedings** (debates, speeches, discussions), NOT enacted legislation. For Dutch enacted legislation, use:
- **NL/wetten.overheid.nl** — Consolidated Dutch laws (45K+ regulations)
- **NL/Staatsblad** — Dutch Official Gazette (newly published laws)

## API Details

### Base URL
```
https://gegevensmagazijn.tweedekamer.nl/OData/v4/2.0
```

### Key Entities

| Entity | Description |
|--------|-------------|
| Verslag | Debate transcripts (XML full text via /resource) |
| Vergadering | Meeting/session metadata |
| Document | Parliamentary documents (PDF/DOCX) |
| Zaak | Legislative cases and matters |
| Persoon | MPs and other persons |

### Example Queries

List recent transcripts:
```
GET /Verslag?$filter=Verwijderd eq false&$top=10&$orderby=GewijzigdOp desc
```

Fetch full text XML:
```
GET /Verslag({guid})/resource
```

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (12 with full text)
python bootstrap.py bootstrap --sample

# Full bootstrap (all transcripts)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Output Schema

| Field | Description |
|-------|-------------|
| _id | Verslag GUID |
| _source | "NL/TweedeKamer" |
| _type | "parliamentary_proceedings" |
| title | Session/debate title |
| text | Full debate transcript (extracted from XML) |
| date | Date of the session |
| vergaderjaar | Parliamentary year (e.g., "2024-2025") |
| vergaderingnummer | Session number |
| soort | Type (Tussenpublicatie, Voorpublicatie, etc.) |

## License

[Public Domain](https://data.overheid.nl/licenties) — Dutch Open Government Data, free for reuse.

## Documentation

- [Open Data Portal](https://opendata.tweedekamer.nl)
- [OData API Documentation](https://opendata.tweedekamer.nl/documentatie/odata-api)
- [Data Model](https://opendata.tweedekamer.nl/documentatie/informatiemodel)
