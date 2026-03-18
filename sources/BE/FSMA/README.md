# BE/FSMA - Belgian Financial Services and Markets Authority

## Overview

This source fetches administrative sanctions and settlement agreements from the Belgian Financial Services and Markets Authority (FSMA / Autorité des services et marchés financiers).

## Data Source

- **Main page**: https://www.fsma.be/fr/reglements-transactionnels
- **Language versions**: French, Dutch, English
- **Update frequency**: As decisions are published

## Data Types

- Settlement agreements (règlements transactionnels / minnelijke schikkingen)
- Sanctions Committee decisions
- Historical enforcement decisions

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (BE/FSMA/filename) |
| `title` | Decision title |
| `text` | Full text of the decision (extracted from PDF) |
| `date` | Decision date (ISO 8601) |
| `url` | Link to original PDF |
| `language` | Language of the document (fr/nl) |
| `decision_type` | Type: settlement, sanction, decision |
| `entity_name` | Sanctioned entity name |
| `amount` | Fine or settlement amount in EUR |

## Usage

```bash
# Fetch sample (15 records)
python bootstrap.py bootstrap --sample

# Full fetch (all decisions)
python bootstrap.py bootstrap --full
```

## Requirements

- Python 3.8+
- pdfplumber or pypdf
- beautifulsoup4
- requests

## License

Belgian Federal Government Open Data
