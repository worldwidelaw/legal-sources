# FJ/SupremeCourt — Fiji Courts Online Decisions

Official Fiji judiciary decisions from [judiciary.gov.fj](https://judiciary.gov.fj).

## Coverage

- **Courts**: Supreme Court, Court of Appeal, High Court (Civil, Criminal, Employment, Family), Magistrates Court, and various tribunals
- **Period**: 2016–present (~7,400 judgments)
- **Data type**: case_law
- **Language**: English
- **Pre-2015**: Available on PacLII (not covered by this source)

## Data Access

Uses the WordPress REST API with a custom `judgments` post type:

```
GET https://judiciary.gov.fj/wp-json/wp/v2/judgments?per_page=100&page=1
```

Each judgment includes:
- Case metadata (title, date, court taxonomy)
- ACF custom field with PDF document URL
- Full text extracted from linked PDF

## Authentication

None required — public API.

## Rate Limits

0.5 requests/second with 1-second delay between pages.
