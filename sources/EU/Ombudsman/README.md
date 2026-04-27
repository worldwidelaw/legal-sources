# EU/Ombudsman — European Ombudsman Decisions

## Overview

Fetches decisions from the European Ombudsman via the REST API at `ombudsman.europa.eu/rest/documents`. ~5,600 decisions on complaints about EU institutional maladministration.

## Data Source

Public REST API, no authentication required. The list endpoint returns full text content as HTML, which is stripped to plain text.

## Coverage

- **Records**: ~5,640 decisions
- **Period**: ~2001–present
- **Topics**: Transparency, access to documents, recruitment, contracts, fundamental rights
- **Language**: English

## Fields

| Field | Description |
|-------|-------------|
| `_id` | EU-OMB-{techKey} |
| `title` | Decision title |
| `text` | Full text (HTML stripped) |
| `date` | Decision date |
| `case_ref` | Case reference (e.g., 555/2025/MAS) |
| `case_id` | Internal case ID |
| `summary` | Case summary |

## Usage

```bash
python bootstrap.py test               # Test API connectivity
python bootstrap.py bootstrap --sample # Fetch 15 sample records
python bootstrap.py bootstrap          # Full dataset (~5,640 decisions)
```

## License

[EUR-Lex legal notice](https://eur-lex.europa.eu/content/legal-notice/legal-notice.html) — EU institution publications, reuse authorised with attribution.
