# CH/COMCO - Swiss Competition Commission (WEKO)

## Data Source

The **Wettbewerbskommission (WEKO)** / **Competition Commission (COMCO)** is Switzerland's competition authority, responsible for enforcing the Cartel Act and merger control.

- **Website**: https://www.weko.admin.ch
- **Decisions**: https://www.weko.admin.ch/de/entscheide
- **Data format**: PDF documents

## Coverage

The source includes:
- **Competition decisions** (Verfügungen) - antitrust cases, cartels, abuse of dominance
- **Merger control decisions** - merger notifications and clearances
- **Final reports** (Schlussberichte) - market investigations
- **Advisory opinions** (Stellungnahmen) - legal opinions on competition matters
- **Recommendations** (Anregungen) - recommendations to parties

## Languages

Documents are published in German, French, and Italian (official Swiss languages).

## Technical Details

- **Method**: HTML scraping + PDF download + text extraction
- **Dependencies**: pdfplumber or pypdf for PDF text extraction
- **Rate limit**: 0.5 requests/second

## Usage

```bash
# Fetch sample records
python bootstrap.py bootstrap --sample --count 15

# Full fetch
python bootstrap.py bootstrap --full
```

## Schema

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (CH/COMCO/{doc_id}) |
| `title` | Decision title |
| `text` | Full text of the decision |
| `date` | Decision date (YYYY-MM-DD) |
| `url` | Link to PDF document |
| `decision_type` | Type: decision, final_report, opinion, merger, advisory |
| `language` | Document language (de/fr/it) |

## License

Public domain - Swiss federal government publications.
