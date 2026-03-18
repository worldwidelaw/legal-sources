# CZ/UOOU - Czech Data Protection Authority

## Source Information

- **Name**: Úřad pro ochranu osobních údajů (ÚOOÚ)
- **English Name**: Office for Personal Data Protection
- **Country**: Czech Republic (CZ)
- **Website**: https://uoou.gov.cz
- **Data Type**: Regulatory decisions (completed inspections)

## Description

The Czech Office for Personal Data Protection (ÚOOÚ) is the supervisory
authority responsible for enforcing data protection legislation in the Czech
Republic, including GDPR.

This source fetches completed inspection summaries from the ÚOOÚ website,
including:

- **Personal data protection violations** - GDPR enforcement decisions
- **Unsolicited commercial communications** - violations of Act No. 480/2004 Sb.
- **Schengen information systems** - access and data quality inspections

## Data Coverage

- **Years**: 2001 - present
- **Content**: Full text inspection summaries with case numbers and outcomes
- **Updates**: Periodic publication of completed inspections

## Technical Details

### Access Method

HTML scraping of the public website. The ÚOOÚ does not provide an API.

### Page Structure

```
/cinnost/ochrana-osobnich-udaju/ukoncene-kontroly/
├── kontroly-za-rok-2024-1/
│   ├── kontroly-za-rok-2024/
│   │   ├── obchodni-spolecnost-0003724
│   │   └── ...
│   ├── nevyzadana-obchodni-sdeleni-2024/
│   └── schengen-2024/
├── kontroly-za-rok-2023/
│   └── ...
└── ...
```

### Rate Limiting

- 1.5 seconds between requests
- Respectful crawling with proper User-Agent

## Usage

```bash
# Test fetch (3 documents)
python3 bootstrap.py

# Sample bootstrap (15 documents)
python3 bootstrap.py bootstrap --sample

# Full bootstrap (all available decisions)
python3 bootstrap.py bootstrap
```

## Schema

| Field | Type | Description |
|-------|------|-------------|
| `_id` | string | Unique ID (UOOU-{case_number}) |
| `_source` | string | "CZ/UOOU" |
| `_type` | string | "regulatory_decision" |
| `title` | string | Inspection title |
| `case_number` | string | Official case reference (e.g., "00037/24") |
| `text` | string | Full text of inspection summary |
| `date` | string | Date modified (ISO 8601) |
| `year` | integer | Inspection year |
| `category` | string | personal_data, commercial_communications, or schengen |
| `url` | string | Original page URL |
| `language` | string | "cs" |

## Notes

- Inspections published before GDPR (May 25, 2018) may not be directly applicable
  to current requirements.
- Company names are typically anonymized as "Obchodní společnost" (Commercial company)
- Fine amounts are included in the inspection text
