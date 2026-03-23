# EU/EFSA - European Food Safety Authority

## Overview

This source fetches scientific opinions, assessments, guidance documents, and technical reports from the European Food Safety Authority (EFSA).

## Data Source

**API**: CrossRef API with DOI prefix filter `10.2903` (EFSA Journal)

The EFSA Journal is an open access peer-reviewed scientific journal that publishes all EFSA scientific outputs.

## Document Types

- **Scientific Opinions**: Risk assessments and safety evaluations
- **Technical Reports**: Detailed technical analyses
- **Scientific Reports**: Research findings and data summaries
- **Guidance Documents**: Methodological guidelines
- **Statements**: Official EFSA positions
- **Peer Reviews**: Evaluations of pesticide active substances

## Coverage

- **Total Publications**: ~11,500+ documents
- **Date Range**: 2003 - present
- **Update Frequency**: Daily (new publications indexed within days)

## Schema

Each normalized document contains:

| Field | Type | Description |
|-------|------|-------------|
| `_id` | string | Unique identifier (DOI-based) |
| `_source` | string | Always `EU/EFSA` |
| `_type` | string | Always `doctrine` |
| `doi` | string | Digital Object Identifier |
| `title` | string | Full document title |
| `text` | string | Scientific abstract/summary |
| `date` | string | Publication date (YYYY-MM-DD) |
| `url` | string | Link to full publication |
| `authors` | array | List of authors/panels |
| `journal` | string | Journal name (EFSA Journal) |
| `document_type` | string | Type classification |
| `subjects` | array | Subject categories |

## Usage

```bash
# Quick test (5 documents)
python3 bootstrap.py

# Sample mode (15 documents)
python3 bootstrap.py bootstrap --sample

# Full bootstrap (50 documents)
python3 bootstrap.py bootstrap
```

## API Notes

- **Rate Limit**: 50 requests/second (CrossRef polite pool)
- **Authentication**: None required
- **Pagination**: Uses cursor-based deep paging

## Full Text

The `text` field contains the scientific abstract which summarizes the EFSA's assessment, conclusions, and recommendations. For scientific doctrine, this abstract is the primary authoritative content.

Full PDF text is available via Wiley but requires TDM authentication. The abstracts provided via CrossRef contain the essential scientific conclusions.
