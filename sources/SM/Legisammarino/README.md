# SM/Legisammarino — San Marino Official Legislation

Fetches San Marino legislation from the Official Bulletin (Bollettino Ufficiale).

## Data Source

- **URL**: https://www.bollettinoufficiale.sm
- **Coverage**: 2012-present (electronic format)
- **Language**: Italian
- **License**: San Marino Open Government Data

## Document Types

- **Legge Costituzionale** — Constitutional Law
- **Legge Qualificata** — Qualified Law
- **Legge Ordinaria** — Ordinary Law
- **Decreto Delegato** — Delegated Decree
- **Decreto Legge** — Decree Law
- **Decreto Consiliare** — Council Decree
- **Decreto Reggenziale** — Regency Decree
- **Regolamento** — Regulation
- **Delibera di Ratifica** — Ratification Deliberation

## Strategy

1. Iterate through monthly bulletin archives (2012-present)
2. Parse HTML table of contents to extract document metadata and links
3. Download individual PDFs for each document
4. Extract full text using pdfplumber

## Endpoints

- **Bulletin Search**: `GET /on-line/home/parte-ufficiale/ricerca.html?P0_operation=getBollettino&P0_anno={year}&P0_mese={month}`
- **Document**: `GET /on-line/RicercaBU?operation=getDocBU&id={doc_id}`

## Usage

```bash
# Quick connectivity test
python bootstrap.py test

# Fetch 12 sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all documents from 2012)
python bootstrap.py bootstrap

# Incremental update (documents since last run)
python bootstrap.py update
```

## Requirements

- Python 3.8+
- pdfplumber (for PDF text extraction)

```bash
pip install pdfplumber
```

## Sample Output

```json
{
  "_id": "SM-L-16-2026",
  "_source": "SM/Legisammarino",
  "_type": "legislation",
  "_fetched_at": "2026-02-19T17:36:20.362000+00:00",
  "title": "Legge Ordinaria n. 16 del 04/02/2026 - Indicatore della Condizione Economica...",
  "text": "REPUBBLICA DI SAN MARINO\nNoi Capitani Reggenti...",
  "date": "2026-02-04",
  "url": "https://www.bollettinoufficiale.sm/on-line/RicercaBU?operation=getDocBU&id=...",
  "document_type": "Legge Ordinaria",
  "number": "16",
  "language": "it"
}
```

## Notes

- Electronic bulletin started January 1, 2012 per Legge Qualificata n. 2/2010
- Monthly bulletins organized by 12 document type categories
- All documents served as PDF files; text extracted programmatically
- Rate limited to 1 request/second to avoid overwhelming the server

## License

[Open Government Data](https://www.bollettinoufficiale.sm) — official bulletin of the Republic of San Marino, per Legge Qualificata n. 2/2010.
