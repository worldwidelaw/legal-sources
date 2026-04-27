# NZ/Legislation - New Zealand Legislation

Official collection of New Zealand legislation from [legislation.govt.nz](https://www.legislation.govt.nz).

## Data Coverage

- **Acts**: ~17,500 documents
  - Public Acts
  - Local Acts
  - Private Acts
  - Imperial Acts (historical UK laws still in force)
  - Provincial Acts (historical)

- **Bills**: ~1,800 documents
  - Government Bills
  - Members' Bills
  - Local Bills
  - Private Bills

- **Secondary Legislation**: ~20,000 documents
  - Regulations
  - Orders
  - Rules
  - Other instruments

- **Amendment Papers (SOPs)**: ~2,700 documents

## Data Access Method

This scraper uses the public sitemap and XML files:

1. Parse `sitemap.xml` to discover all legislation URLs
2. Append `.xml` to each URL to get the full XML document
3. Extract plain text from the structured XML

No API key is required for this approach.

### Alternative: Official API

An official REST API is available at `api.legislation.govt.nz` but requires an API key.
To request access, email: contact@pco.govt.nz

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample data (10-15 records)
python bootstrap.py bootstrap --sample

# Full bootstrap (all ~42,000 documents)
python bootstrap.py bootstrap
```

## Schema

Key fields in normalized records:

| Field | Description |
|-------|-------------|
| `_id` | Unique ID: `{type}_{subtype}_{year}_{number}` |
| `title` | Official title |
| `text` | Full text content |
| `legislation_type` | act, bill, secondary-legislation, amendment-paper |
| `legislation_subtype` | public, local, private, government, members, etc. |
| `year` | Year of enactment/introduction |
| `number` | Legislation number |
| `date` | Assent/publication date |
| `url` | Source URL |

## License

[New Zealand Government Open Access and Licensing framework (NZGOAL)](https://www.data.govt.nz/manage-data/policies/nzgoal/) — Crown Copyright, re-use permitted under CC BY 4.0 terms.
