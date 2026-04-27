# NL/AP - Dutch Data Protection Authority (Autoriteit Persoonsgegevens)

The Dutch Data Protection Authority (Autoriteit Persoonsgegevens, AP) is the supervisory authority for data protection in the Netherlands. The AP enforces GDPR and publishes enforcement decisions, fines, and guidelines.

## Data Source

- **Main URL**: https://www.autoriteitpersoonsgegevens.nl
- **Documents**: https://www.autoriteitpersoonsgegevens.nl/documenten
- **Sanctions**: https://www.autoriteitpersoonsgegevens.nl/boetes-en-andere-sancties

## Data Types

- **Enforcement decisions** (boetebesluiten): GDPR fine decisions with full reasoning
- **FOI decisions** (Woo-besluiten): Freedom of Information Act decisions
- **Policy advice** (toetsen): Regulatory assessments of proposed legislation
- **Guidelines**: Guidance documents for data controllers

## Technical Notes

- Documents are listed in HTML pages with pagination
- Full text is available via PDF attachments on each document page
- PDF text extraction using `pypdf`
- Rate limiting: 2 seconds between requests
- Language: Dutch (nl)

## Usage

```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full fetch
python bootstrap.py bootstrap
```

## Schema

| Field | Description |
|-------|-------------|
| _id | Unique identifier (NL-AP-{slug}) |
| title | Document title |
| text | Full text content (from PDF) |
| date | Publication date (ISO format) |
| url | Link to original document page |
| document_type | Type: fine, foi_decision, advice, guidance |
| topics | Related topics/themes |
| summary | Brief description |
| pdf_url | Direct link to PDF file |

## License

[Open Government Data](https://data.overheid.nl/licenties) — Dutch government publications, free for reuse.
