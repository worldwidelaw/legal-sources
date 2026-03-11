# FR/CADA - Commission d'accès aux documents administratifs

French independent authority handling document access requests.

## Data Source

- **URL**: https://cada.data.gouv.fr
- **API**: JSON REST API at `/api/search` and `/api/{id}/`
- **License**: Open Licence Etalab

## Coverage

- **Volume**: 60,000+ opinions
- **Period**: 1984 to present
- **Language**: French

## Usage

```bash
# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Show statistics
python3 bootstrap.py stats
```

## Data Schema

| Field | Description |
|-------|-------------|
| opinion_id | CADA opinion identifier (e.g., "20164423") |
| text | Full text of the opinion |
| date | Session date (YYYY-MM-DD) |
| subject | Description of the document request |
| administration | Name of the administration involved |
| meanings | CADA's recommendation (Favorable, Défavorable, etc.) |
| topics | Subject matter categories |
| tags | Additional classification tags |

## Notes

- CADA opinions are non-binding but highly influential
- Most opinions concern local government, ministries, and public institutions
- Topics cover everything from public procurement to personal data access
