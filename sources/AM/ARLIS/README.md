# AM/ARLIS - Armenian Legal Information System

Armenian Legal Information System (ARLIS) data fetcher.

## Data Source

- **URL**: https://www.arlis.am
- **Data Types**: Legislation (laws, government decisions, ministerial orders, treaties)
- **Coverage**: 1998-present (154,000+ documents)
- **Languages**: Armenian (primary), Russian, English (some documents)
- **License**: Public government data

## Data Access Strategy

1. **Metadata Index**: Downloaded from OpenData Armenia (data.opendata.am)
   - JSONL format with document metadata (ID, title, dates, status, etc.)
   - ~155K document records

2. **Full Text HTML**: Fetched from ARLIS print endpoint
   - URL: `https://www.arlis.am/hy/acts/{uniqid}/print/act`
   - Clean HTML for text extraction

3. **PDF Fallback**: If HTML fails, extracts from PDF
   - URL: `https://pdf.arlis.am/{uniqid}`
   - Uses PyMuPDF for extraction

## Document Types

- Օdelays (Laws)
- Որdelays (Decisions)
- Հdelays (Orders)
- International treaties
- Constitutional amendments

## Usage

```bash
# Quick connectivity test
python bootstrap.py test

# Fetch sample records (12 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (all 154K+ documents)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Sample Output

```json
{
  "_id": "6611",
  "_source": "AM/ARLIS",
  "_type": "legislation",
  "title": "ՀՀ Կdelays...",
  "text": "[Full document text in Armenian]",
  "date": "1998-04-02",
  "act_type": "Որdelays",
  "act_status": "Գdelays է",
  "enactment_organ": " delays"
}
```
