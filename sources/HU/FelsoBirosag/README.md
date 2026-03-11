# HU/FelsoBirosag - Hungarian Supreme Court (Kúria)

## Data Source
- **URL**: https://kuria-birosag.hu
- **License**: Public Domain
- **Language**: Hungarian (hu)
- **Type**: Case Law

## Coverage

This source collects court decisions from the Hungarian Supreme Court (Kúria), including:

- **Electoral decisions** (`/hu/valhat/`) - Voting and election-related cases
- **Legal unity decisions** (`/hu/joghat/`) - Precedent-setting rulings (JEH, BJE, PJE, KJE, MJE)
- **Municipal cases** (`/hu/onkugy/`) - Local government disputes
- **Referendum cases** (`/hu/nepszavugy/`) - National referendum questions
- **ECHR cases** (`/hu/ejeb/`) - European Court of Human Rights related cases
- **Constitutional cases** (`/hu/alkotmbir-hat/`) - Constitutional court decisions
- **Legal unity complaints** (`/hu/jogegysegi-panasz/`) - Uniformity complaint decisions
- **Curia decisions** (`/hu/kuriai-dontesek/`) - Monthly decision compilations
- **Assembly cases** (`/hu/gyulhat/`) - Freedom of assembly cases
- **Collegial opinions** (`/hu/kollvel/`) - Advisory opinions

## Implementation

### Data Access Method
Uses the XML sitemap at `/hu/sitemap.xml` to discover all decision URLs, then scrapes individual decision pages for full text.

### Technical Notes
- Uses `curl` subprocess for HTTP requests (workaround for Python SSL/LibreSSL compatibility)
- Full text is extracted from the `<div class="field--name-body">` HTML element
- Rate limiting: 1.5 seconds between requests
- Total available decisions: ~1,900+ (as of Feb 2026)

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (HU_KURIA_{case_number}) |
| `_source` | Source ID (HU/FelsoBirosag) |
| `_type` | Always "case_law" |
| `_fetched_at` | ISO 8601 timestamp |
| `title` | Decision title |
| `text` | Full text of the decision |
| `date` | Decision date (YYYY-MM-DD) |
| `url` | Link to original source |
| `decision_number` | Official decision number (e.g., "1/2012 BJE") |
| `case_number` | Case reference number (e.g., "Kvk.I.37.556/2012/2") |
| `decision_type` | Type classification |

## Usage

```bash
# Fetch sample records
python3 bootstrap.py bootstrap --sample --count 12

# Fetch all decisions (full run)
python3 bootstrap.py fetch

# Fetch updates since date
python3 bootstrap.py updates --since 2024-01-01
```

## Dependencies

- beautifulsoup4 (for HTML parsing)
- curl (system command for HTTP requests)

Install with:
```bash
pip install beautifulsoup4
```

## Sample Statistics (Feb 2026)
- Records fetched: 12
- Average text length: 18,056 chars/document
- Total text: 216,677 characters
