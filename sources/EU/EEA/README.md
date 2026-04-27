# EU/EEA - European Environment Agency

## Overview

The European Environment Agency (EEA) provides independent information on the environment for those involved in developing, adopting, implementing, and evaluating environmental policy, as well as the general public.

## Data Source

- **URL**: https://www.eea.europa.eu
- **API**: Plone JSON API (`++api++`)
- **Data Types**: Doctrine (briefings, reports, assessments)

## Approach

The fetcher uses the EEA's Plone REST API to:

1. Query the publications search endpoint at `https://www.eea.europa.eu/++api++/en/analysis/publications/@search`
2. Filter by content types: `briefing` and `web_report`
3. Fetch individual publication details
4. Extract full text from the `blocks` structure (Plone's Slate editor format)

## Content Types

- **briefing**: Short, online assessments on specific environmental topics
- **web_report**: Longer, comprehensive reports presented as web documents

Note: `report_pdf` types are excluded because they contain minimal inline text (the content is in PDF files).

## API Details

### Search Endpoint
```
GET https://www.eea.europa.eu/++api++/en/analysis/publications/@search?portal_type=briefing&portal_type=web_report&b_size=50&b_start=0
```

### Publication Detail
```
GET https://www.eea.europa.eu/++api++/en/analysis/publications/{publication-id}
```

### Response Structure
Publications contain a `blocks` object with Slate editor content. Text is extracted from blocks with `@type: "slate"` and their `plaintext` field.

## Usage

```bash
# Test with 3 documents
python3 bootstrap.py

# Bootstrap with 15 sample documents
python3 bootstrap.py bootstrap --sample

# Bootstrap with 50 documents
python3 bootstrap.py bootstrap
```

## Rate Limits

- 1.5 seconds between publication requests
- 1.0 seconds between search pages

## Output Schema

```json
{
  "_id": "EEA-{uid}",
  "_source": "EU/EEA",
  "_type": "doctrine",
  "_fetched_at": "ISO timestamp",
  "uid": "EEA unique ID",
  "title": "Publication title",
  "description": "Short description",
  "text": "Full text content",
  "date": "YYYY-MM-DD",
  "url": "Publication URL",
  "content_type": "briefing|web_report",
  "topics": ["topic1", "topic2"],
  "geo_coverage": ["EU", "country1"],
  "language": "en"
}
```

## Coverage

As of 2026-03, there are approximately:
- 148 briefings
- ~950 web reports

Total: ~1,100 publications with full text content.

## License

[CC BY 2.5 DK](https://creativecommons.org/licenses/by/2.5/dk/) — EEA content is generally available under Creative Commons Attribution.
