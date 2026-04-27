# RU/Sudact - Russian Court Decisions (sudact.ru)

## Overview
Fetches case law from sudact.ru, the largest open database of Russian court
decisions (sudebnye i normativnye akty RF). Covers:
- **General jurisdiction courts** (regular) across all 85+ federal subjects
- **Arbitration courts** (arbitral)
- **Magistrate courts** (magistrate)
- **Supreme Court** (vsrf)

## Data Access
- **Discovery**: XML sitemap index at `https://sudact.ru/sitemap.xml`
  - Two sitemap files with ~98K decision URLs
- **Full text**: Embedded in HTML on individual decision pages
- **Metadata**: JSON-LD (schema.org Article) with headline, court, date
- **Auth**: None required (open access)

## Record Schema
| Field | Description |
|-------|-------------|
| `_id` | `RU-Sudact-{doc_id}` |
| `title` | Decision title (type, number, date, case number) |
| `text` | Full text of the court decision |
| `date` | Decision date (ISO 8601) |
| `court` | Court name |
| `court_type` | `arbitration`, `general_jurisdiction`, `magistrate`, `supreme_court` |
| `case_number` | Case number |
| `url` | Link to original on sudact.ru |

## Usage
```bash
python bootstrap.py bootstrap --sample   # Fetch 15 sample records
python bootstrap.py bootstrap             # Full fetch (~98K records)
```

## Notes
- Rate limited to 1 request/second
- robots.txt allows crawling of decision pages
- No explicit ToS prohibiting automated access
- Data aggregated from GAS Pravosudie (sudrf.ru)

## License

[Open Access](https://sudact.ru) — court decisions are public records under Russian law. Data aggregated from the State Automated System "Pravosudie".
