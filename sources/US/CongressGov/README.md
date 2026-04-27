# US/CongressGov - Congressional Bills

Fetches U.S. Congressional bills, resolutions, and amendments from the Congress.gov API maintained by the Library of Congress.

## Data Source

- **API**: https://api.congress.gov
- **Documentation**: https://github.com/LibraryOfCongress/api.congress.gov
- **Coverage**: All Congressional bills from 1789 to present
- **Data Types**: Legislation (bills, resolutions, amendments)
- **License**: Public Domain (17 USC § 105)

## Authentication

The API requires an API key:

1. **DEMO_KEY**: Works for testing with rate limits
2. **Custom key**: Get one at https://api.congress.gov/sign-up/

Set in environment:
```bash
export CONGRESS_API_KEY=your_api_key_here
```

## Rate Limits

- 5,000 requests per hour
- The fetcher includes built-in delays (0.5s between requests)

## Usage

```bash
# Fetch sample data
python3 bootstrap.py bootstrap --sample

# Fetch specific number of records
python3 bootstrap.py fetch --count 50 --congress 118

# Fetch updates since a date
python3 bootstrap.py fetch --since 2024-01-01

# Validate sample data
python3 bootstrap.py validate
```

## Data Schema

Each record contains:

- `_id`: Unique identifier (e.g., "118-hr-1")
- `_source`: "US/CongressGov"
- `_type`: "legislation"
- `_fetched_at`: ISO 8601 timestamp
- `title`: Bill title
- `text`: Full text of the bill (extracted from XML)
- `date`: Introduction date
- `url`: Link to congress.gov
- `congress`: Congress number
- `bill_type`: HR, S, HJRES, SJRES, etc.
- `bill_number`: Bill number
- `sponsor`: Sponsor name
- `cosponsors_count`: Number of cosponsors
- `policy_area`: Policy category
- `origin_chamber`: House or Senate
- `latest_action`: Most recent action text

## Bill Types

- `hr` - House Bills
- `s` - Senate Bills
- `hjres` - House Joint Resolutions
- `sjres` - Senate Joint Resolutions
- `hconres` - House Concurrent Resolutions
- `sconres` - Senate Concurrent Resolutions
- `hres` - House Simple Resolutions
- `sres` - Senate Simple Resolutions

## Full Text Extraction

The API provides URLs to bill text in multiple formats (XML, HTML, PDF).
This fetcher:

1. Retrieves the XML version when available
2. Parses the XML structure to extract clean text
3. Falls back to HTML stripping if needed
4. Removes markup while preserving content

## License

[Public domain](https://www.law.cornell.edu/uscode/text/17/105) — US government works under 17 U.S.C. § 105.
