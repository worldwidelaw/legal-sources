# BA/SluzbenGlasnik - Bosnia and Herzegovina Official Gazette

## Overview

**Source:** Službeni glasnik BiH (Official Gazette of Bosnia and Herzegovina)
**URL:** http://www.sluzbenilist.ba
**Country:** Bosnia and Herzegovina
**Data Type:** Legislation
**Authentication:** None required for state-level gazette
**License:** Open Government Data

## Coverage

This source covers **state-level legislation** from Bosnia and Herzegovina:

- **Službeni glasnik BiH** - State-level official gazette
- **Službeni glasnik BiH – Međunarodni ugovori** - International treaties

### Not Covered (Subscription Required)

- Službene novine Federacije BiH (Federation of BiH)
- Službene novine Kantona Sarajevo (Sarajevo Canton)
- Other cantonal gazettes

### Temporal Coverage

- 1997 to present (from website availability)

### Languages

- Bosnian
- Croatian
- Serbian

## Document Types

- Zakoni (Laws)
- Odluke (Decisions)
- Uredbe (Regulations/Decrees)
- Pravilnici (Ordinances)
- Ugovori (Treaties - for MU type)

## Technical Details

### API Access

No official API. Data is fetched via HTML scraping:

1. **Search Endpoint**: `/search/searchresult?naziv={query}`
   - Returns list of documents with metadata
   - State-level documents have direct links; Federation/Canton require login

2. **Document Endpoint**: `/page/akt/{doc_id}`
   - Returns full HTML page with document content
   - Document ID is base64-encoded

### Rate Limiting

- 2 second delay between requests
- Respectful user agent

### SSL Notes

The website has SSL certificate issues (self-signed certificate in chain).
The fetcher uses `verify=False` to handle this.

## Usage

```bash
# Search for documents
python bootstrap.py search zakon

# Fetch sample documents
python bootstrap.py bootstrap --sample --count 12

# Custom output directory
python bootstrap.py bootstrap --sample --output ./my_samples
```

## Data Schema

```json
{
  "_id": "BA-SG-11-26-abc12345",
  "_source": "BA/SluzbenGlasnik",
  "_type": "legislation",
  "_fetched_at": "2026-02-16T00:00:00Z",
  "title": "ODLUKA O IMENOVANJU...",
  "text": "Na osnovu člana 17...",
  "date": "2026-02-10",
  "url": "http://www.sluzbenilist.ba/page/akt/abc123",
  "gazette_type": "BiH",
  "gazette_number": "11/26",
  "gazette_year": 2026,
  "issuer": "VIJEĆE MINISTARA BOSNE I HERCEGOVINE",
  "language": "bs"
}
```

## Notes

- Bosnia and Herzegovina has a complex federal structure with multiple official gazettes
- State-level content is freely accessible
- Entity (Federation BiH, Republika Srpska) and cantonal content requires subscription
- The OHR (Office of the High Representative) maintains English translations at ohr.int/laws-of-bih/
