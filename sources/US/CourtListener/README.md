# US/CourtListener - Federal and State Case Law

Data source for CourtListener, a free legal research platform from the Free Law Project.

## Data Coverage

- **Federal Courts**: Supreme Court (SCOTUS), Circuit Courts of Appeals, District Courts, Bankruptcy Courts
- **State Courts**: Supreme Courts, Appellate Courts, Trial Courts
- **Data Types**: Court opinions with full text, PACER docket data, oral arguments
- **Volume**: Millions of opinions spanning decades

## Authentication

CourtListener requires an API token for access. The free tier allows 5,000 queries per hour.

1. Create an account at https://www.courtlistener.com/sign-in/
2. Find your API token in your profile
3. Create `.env` file from `.env.template` and add your token

## Usage

```bash
# Fetch sample records (15 opinions)
python bootstrap.py bootstrap --sample

# Fetch last 30 days of opinions
python bootstrap.py bootstrap --recent

# Fetch updates since a specific date
python bootstrap.py updates --since 2024-01-01

# Validate sample records
python bootstrap.py validate
```

## API Reference

- Base URL: https://www.courtlistener.com/api/rest/v4
- Documentation: https://www.courtlistener.com/help/api/rest/
- Case Law API: https://www.courtlistener.com/help/api/rest/case-law/
- Rate Limit: 5,000 queries/hour (authenticated)

## Data Schema

Each record contains:
- `_id`: Unique identifier (e.g., "cl-opinion-12345")
- `_source`: "US/CourtListener"
- `_type`: "case_law"
- `title`: Case name
- `text`: Full text of the opinion (cleaned of HTML)
- `date`: Date filed
- `url`: Link to CourtListener page
- `court`: Court identifier
- `author`: Author of the opinion
- `opinion_type`: Type of opinion (lead, dissent, concurrence, etc.)
- `citations`: List of citations

## License

[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) — Free Law Project dedicates data to public domain.
