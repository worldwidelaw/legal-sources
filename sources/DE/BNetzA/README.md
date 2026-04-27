# DE/BNetzA - German Federal Network Agency Decisions

## Source Information

- **Name**: Bundesnetzagentur Decision Database (Beschlussdatenbank)
- **URL**: https://www.bundesnetzagentur.de/DE/Beschlusskammern/BDB/start.html
- **Country**: Germany
- **Type**: Regulatory decisions
- **Language**: German (de)

## Description

The Bundesnetzagentur (Federal Network Agency) is the German regulatory authority for electricity, gas, telecommunications, postal services, and railway markets.

The decision database (Beschlussdatenbank) contains regulatory decisions from the ruling chambers (Beschlusskammern), dating from 1998 to present. As of 2025, the database contains approximately 19,217 decisions.

## Ruling Chambers

| Code | Name | Focus |
|------|------|-------|
| GBK | Große Beschlusskammer Energie | Grand Ruling Chamber for Energy |
| BK1 | Beschlusskammer 1 | Postal services regulation |
| BK2 | Beschlusskammer 2 | Subscriber number access |
| BK3 | Beschlusskammer 3 | Telecommunications market regulation |
| BK4 | Beschlusskammer 4 | Energy network access (electricity/gas) |
| BK5 | Beschlusskammer 5 | Interconnection, facility access |
| BK6 | Beschlusskammer 6 | Metering, energy data communication |
| BK7 | Beschlusskammer 7 | Market transparency, capacity allocation |
| BK8 | Beschlusskammer 8 | Revenue caps (electricity) |
| BK9 | Beschlusskammer 9 | Revenue caps (gas) |
| BK10 | Beschlusskammer 10 | Market rules, balancing energy |
| BK11 | Beschlusskammer 11 | Incentive regulation |

## Data Access

The fetcher retrieves decisions through:

1. **HTML Search Pages**: The search interface at `SiteGlobals/Forms/Suche/BDB/Suche_BeschlussDB_Formular.html` provides paginated results with decision metadata
2. **PDF Downloads**: Full decision texts are available as PDF documents

There is no official REST API. The fetcher scrapes the search results and downloads PDFs.

## Full Text Extraction

Decisions are published as PDF documents. Full text is extracted using the `pypdf` library.

## License

> ⚠️ **Commercial use restricted.** Any form of commercial use requires consent from the Bundesnetzagentur.

[BNetzA Terms of Use](https://www.bundesnetzagentur.de/EN/General/LegalNotice/LegalNotice_node.html) — private use permitted without authorization.

## Usage

```bash
# Test fetching (3 decisions)
python3 bootstrap.py

# Bootstrap with sample data (12 decisions)
python3 bootstrap.py bootstrap --sample

# Full bootstrap
python3 bootstrap.py bootstrap
```

## Dependencies

- `requests`
- `beautifulsoup4`
- `pypdf` (required for PDF text extraction)

## Schema

| Field | Type | Description |
|-------|------|-------------|
| `_id` | string | Document ID (based on Aktenzeichen) |
| `_source` | string | Always "DE/BNetzA" |
| `_type` | string | Always "regulatory_decision" |
| `title` | string | Aktenzeichen + subject |
| `text` | string | Full text extracted from PDF |
| `date` | string | Decision date (ISO 8601) |
| `aktenzeichen` | string | Case number (e.g., "BK9-25-619-1") |
| `chamber` | string | Ruling chamber code (e.g., "BK9") |
| `subject` | string | Decision subject matter |
| `affected_party` | string | Party affected by decision |
| `applicant` | string | Applicant name |
| `court_reference` | string | Related court case number (if any) |
| `url` | string | Direct PDF download URL |

## Rate Limiting

The fetcher uses 1.5 second delays between PDF downloads to avoid overloading the server.
