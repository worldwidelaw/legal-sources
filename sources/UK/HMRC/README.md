# UK/HMRC - HM Revenue & Customs Tax Manuals

Technical guidance manuals from HMRC covering all UK taxes.

## Data Source

- **URL**: https://www.gov.uk/government/collections/hmrc-manuals
- **API**: GOV.UK Content API (no auth required)
- **License**: Open Government Licence v3.0
- **Update Frequency**: Daily (manuals updated continuously)

## Coverage

- **247 manuals** containing **84,000+ sections**
- Topics include:
  - Capital Gains Tax
  - Income Tax (Employment, Property, Savings & Investment)
  - Corporation Tax
  - VAT (Input Tax, Notices, Reduced Rate)
  - Inheritance Tax
  - Stamp Duty Land Tax
  - Pensions Tax
  - International Tax
  - Compliance & Investigations
  - And many more...

## API Endpoints

1. **Search API** - List all manuals:
   ```
   GET https://www.gov.uk/api/search.json?filter_format=hmrc_manual&count=300
   ```

2. **Content API** - Manual structure:
   ```
   GET https://www.gov.uk/api/content/hmrc-internal-manuals/{manual-slug}
   ```

3. **Content API** - Section content:
   ```
   GET https://www.gov.uk/api/content/hmrc-internal-manuals/{manual-slug}/{section-id}
   ```

## Record Schema

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (manual_slug_section_id) |
| `_source` | "UK/HMRC" |
| `_type` | "doctrine" |
| `title` | Section ID and title |
| `text` | Full text content (HTML stripped) |
| `date` | Last update date |
| `url` | GOV.UK URL |
| `manual_title` | Parent manual name |
| `manual_slug` | Parent manual identifier |
| `section_id` | Section reference number |

## Usage

```bash
# Test API connectivity
python bootstrap.py test

# Fetch sample records (15 records from various manuals)
python bootstrap.py bootstrap --sample

# Fetch more samples
python bootstrap.py bootstrap --sample --count 30
```

## Notes

- No authentication required
- Rate limit: 1 request per second recommended
- HTML content is converted to plain text
- Index/contents pages are skipped (they don't contain substantive text)
