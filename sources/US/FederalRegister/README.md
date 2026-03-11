# US/FederalRegister - Federal Register

Data source for the Federal Register, the daily journal of the United States Government.

## Data Coverage

- **Document Types**: Rules, Proposed Rules, Notices, Presidential Documents
- **Agencies**: All federal executive agencies
- **Date Range**: 1994 to present
- **Volume**: 500-1000+ new documents per week
- **Full Text**: Available via raw text or HTML endpoints

## Authentication

No authentication required. The API is fully public.

## Usage

```bash
# Fetch sample records (15 documents)
python bootstrap.py bootstrap --sample

# Fetch last 30 days of documents
python bootstrap.py bootstrap --recent

# Fetch updates since a specific date
python bootstrap.py updates --since 2024-01-01

# Validate sample records
python bootstrap.py validate
```

## API Reference

- Base URL: https://www.federalregister.gov/api/v1
- Documentation: https://www.federalregister.gov/developers/documentation/api/v1
- No rate limit documented, but use polite delays (0.5s between requests)

## Data Schema

Each record contains:
- `_id`: Unique identifier (e.g., "fr-2026-04135")
- `_source`: "US/FederalRegister"
- `_type`: "legislation"
- `title`: Document title
- `text`: Full text of the document (cleaned)
- `date`: Publication date
- `url`: Link to Federal Register page
- `document_number`: Official document number
- `type`: Document type (Rule, Proposed Rule, Notice, Presidential Document)
- `agencies`: List of issuing agencies
- `cfr_references`: Code of Federal Regulations references
- `citation`: Federal Register citation
- `pdf_url`: Link to PDF version

## Document Types

- **Rule**: Final rules with regulatory effect
- **Proposed Rule**: Proposed rulemaking for public comment
- **Notice**: Agency announcements and informational documents
- **Presidential Document**: Executive orders, proclamations, memoranda
