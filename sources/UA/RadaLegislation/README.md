# UA/RadaLegislation — Ukraine Legislation

Ukrainian legislation from the Verkhovna Rada (Ukrainian Parliament) Open Data Portal.

## Data Source

- **Portal**: [data.rada.gov.ua](https://data.rada.gov.ua)
- **Legislation DB**: [zakon.rada.gov.ua](https://zakon.rada.gov.ua)
- **API Documentation**: [data.rada.gov.ua/open/main/api](https://data.rada.gov.ua/open/main/api)

## Coverage

- **Documents**: 290,000+ legislative documents
- **Years**: 1991 to present
- **Document types**: Laws, decrees, resolutions, orders, regulations
- **Language**: Ukrainian

## API Access

The API uses "OpenData" as User-Agent for anonymous public access:

```bash
# List recent documents
curl -A "OpenData" https://data.rada.gov.ua/laws/main/r.txt

# Get document metadata (JSON)
curl -A "OpenData" https://data.rada.gov.ua/laws/show/4784-20.json

# Get document full text (plain text)
curl -A "OpenData" https://data.rada.gov.ua/laws/show/4784-20.txt
```

## Rate Limits

- **Per minute**: 60 requests
- **Per day**: 100,000 requests
- **Daily bandwidth**: 200 MB
- **Recommended delay**: 5-7 seconds between requests

## License

**CC BY 4.0** (Creative Commons Attribution 4.0)

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full data fetch
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Document Identifiers

Documents are identified by `nreg` (registration number):

- Laws: `4784-20` (number-session)
- Decrees: `168-2026-п` (number-year-type)
- Resolutions: `111/2026` (number/year)
- Orders: `133-2026-р` (number-year-type)

## Schema

| Field | Type | Description |
|-------|------|-------------|
| `nreg` | string | Document registration number |
| `nazva` | string | Document title (Ukrainian) |
| `text` | string | Full text content |
| `orgdat` | int | Document date (YYYYMMDD) |
| `typ` | int | Document type code |
| `status` | int | Document status code |
| `dokid` | int | Internal document ID |
