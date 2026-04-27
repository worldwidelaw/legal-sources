# EU/OLAF - European Anti-Fraud Office

## Overview

This data source fetches news articles and press releases from the European Anti-Fraud Office (OLAF). OLAF is responsible for detecting, investigating, and stopping fraud affecting EU funds, corruption involving EU officials, and serious misconduct within EU institutions.

## Data Type

**doctrine** - Official communications including:
- Press releases about investigations and operations
- News articles about anti-fraud activities
- Cooperation announcements with member states
- Policy and operational updates

## Source URL

https://anti-fraud.ec.europa.eu

## Data Access Method

HTML scraping of the OLAF news section. The website is built on Drupal 11 with structured HTML content.

## Endpoints

- News listing: `/media-corner/news_en`
- Pagination: `?page=N` (0-indexed)
- Individual articles: `/media-corner/news/{slug}-{date}_en`

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (OLAF-{article-slug}) |
| `_source` | Source identifier (EU/OLAF) |
| `_type` | Document type (doctrine) |
| `_fetched_at` | ISO 8601 fetch timestamp |
| `title` | Article title |
| `text` | Full article text content |
| `date` | Publication date (YYYY-MM-DD) |
| `url` | Original article URL |
| `news_type` | Article type (press release or news article) |
| `is_press_release` | Boolean flag for press releases |

## Usage

```bash
# Test the fetcher
python3 sources/EU/OLAF/bootstrap.py

# Fetch sample data (15 records)
python3 sources/EU/OLAF/bootstrap.py bootstrap --sample

# Fetch full data (100 records)
python3 sources/EU/OLAF/bootstrap.py bootstrap
```

## Rate Limiting

- 2 seconds delay between requests
- Maximum 30 seconds timeout per request
- Retry logic with exponential backoff

## License

[EUR-Lex legal notice](https://eur-lex.europa.eu/content/legal-notice/legal-notice.html) — EU agency publications, reuse authorised with attribution.

## Notes

- OLAF's internal databases (IMS, EDES) require authorization and are not publicly accessible
- Full investigation reports are confidential; only published summaries and press releases are available
- Annual reports are available in PDF format from the Publications Office
