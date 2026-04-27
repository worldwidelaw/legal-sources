# UK/FCA - Financial Conduct Authority

UK Financial Conduct Authority (FCA) enforcement notices including decision notices and final notices.

## Data Source

- **Website**: https://www.fca.org.uk
- **Method**: Sitemap parsing + PDF text extraction
- **License**: [Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)

## Data Types

- `regulatory_decisions` - Decision notices and final notices

## Coverage

The FCA publishes two main types of enforcement notices:

1. **Decision Notices** - Issued when the FCA decides to take regulatory action against a firm or individual. The subject can refer the matter to the Upper Tribunal.

2. **Final Notices** - Issued after the subject has either accepted the decision or exhausted appeals. These are the definitive enforcement records.

As of March 2026:
- ~1,800 decision notices
- ~3,550 final notices
- Total: ~5,350 notices spanning 2005-present

## Fields

| Field | Type | Description |
|-------|------|-------------|
| `_id` | string | Unique identifier derived from PDF path |
| `_source` | string | Always "UK/FCA" |
| `_type` | string | Always "regulatory_decisions" |
| `_fetched_at` | string | ISO 8601 fetch timestamp |
| `title` | string | Notice title |
| `text` | string | Full text extracted from PDF |
| `date` | string | Notice date (YYYY-MM-DD) |
| `url` | string | Direct link to PDF |
| `notice_type` | string | "decision_notice" or "final_notice" |
| `subject_name` | string | Name of firm or individual |
| `reference_number` | string | FCA reference number (if available) |

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records
python bootstrap.py bootstrap --sample

# Fetch specific count
python bootstrap.py bootstrap --sample --count 20
```

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — free reuse with attribution.

## Technical Notes

- PDFs are downloaded and text extracted using pdfplumber
- Rate limited to 1 request per second
- Sitemap at https://www.fca.org.uk/sitemap.xml?page=2 contains all notice URLs
- No authentication required
