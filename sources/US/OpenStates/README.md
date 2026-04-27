# US/OpenStates - State Legislation Data

Open States provides comprehensive state-level legislative data for all 50 US states,
plus DC and Puerto Rico. Data is sourced from official state legislative websites
and made available through a REST API.

## Source Information

- **Website**: https://openstates.org
- **API Documentation**: https://docs.openstates.org/api-v3/
- **Data Provider**: Plural Policy (formerly Civic Eagle / Open States)
- **License**: Public Domain (with attribution appreciated)

## Coverage

- **Jurisdictions**: 50 US states + DC + Puerto Rico (52 total)
- **Data Types**: Bills, bill versions (full text), votes, legislators, committees
- **Historical Data**: Varies by state, typically 10-20 years

## Authentication

**API Key Required**: Register for a free key at https://open.pluralpolicy.com/accounts/profile/

Set the environment variable:
```bash
export OPENSTATES_API_KEY=your_api_key_here
```

## Usage

```bash
# Fetch sample records (10+ bills with full text)
python bootstrap.py bootstrap --sample

# Validate samples
python bootstrap.py validate
```

## Data Schema

Each normalized record contains:

- `_id`: Unique identifier (openstates-{uuid})
- `_source`: "US/OpenStates"
- `_type`: "legislation"
- `title`: Bill title
- `text`: Full text of the bill (extracted from version documents)
- `abstract`: Bill summary/abstract
- `date`: Latest action date
- `identifier`: Bill number (e.g., "HB 123")
- `session`: Legislative session
- `jurisdiction`: State name
- `state_code`: Two-letter state code
- `classification`: Bill type (e.g., ["bill"], ["resolution"])
- `url`: Link to official source

## Full Text Retrieval

The Open States API provides links to bill versions (HTML, PDF, XML) rather than
inline text. This fetcher:

1. Queries bills with `include=versions` parameter
2. Downloads version documents (preferring HTML/XML over PDF)
3. Extracts plain text from the document content
4. Stores the text in the normalized `text` field

PDFs are skipped as they require specialized extraction libraries.

## Rate Limits

The API currently does not enforce strict rate limits, but we use 1-second delays
between requests to be respectful.

## License

[Public domain](https://www.law.cornell.edu/uscode/text/17/105) — US government works under 17 U.S.C. § 105. Open States aggregated data is public domain with attribution appreciated.
