# BG/ConstitutionalCourt - Bulgarian Constitutional Court

## Data Source
- **Website**: https://www.constcourt.bg
- **Coverage**: Constitutional Court decisions from 1991 onwards
- **Data Type**: Case law (decisions, rulings, orders)
- **Language**: Bulgarian
- **License**: Public domain (government data)

## Access Method
HTML scraping of the official Constitutional Court website.

### Endpoints Used
1. **Search API**: `GET /?mode=search_acts&year=YYYY`
   - Returns HTML page with acts listed for a given year
   - Extract act IDs from links in format `/bg/act-{id}`

2. **Act Detail Page**: `GET /bg/act-{id}`
   - Full HTML page with document content in `<div id="document-content">`
   - Contains full text, metadata, and judge information

3. **PDF Download**: `GET /generate_document.php?act_id={id}`
   - Alternative PDF version of the document

## Document Types
- **решение** (decision) - Main rulings on constitutional matters
- **определение** (order/ruling) - Procedural orders
- **определение по допустимост** (admissibility ruling)
- **особено мнение** (dissenting opinion)
- **тълкувателни решения** (interpretative decisions)

## Schema
Records include:
- `act_id`: Unique identifier (e.g., "10220")
- `case_number`: Case reference (e.g., "19/2025")
- `act_type`: Type of document
- `date`: Date of the decision (ISO 8601)
- `title`: Document title/type
- `text`: **Full text content** (HTML cleaned)
- `judges`: List of judges on the panel
- `url`: Link to original document

## Rate Limiting
- 1 request per second with burst of 3
- Respectful scraping to avoid overloading the court website

## Notes
- The Constitutional Court has decisions from 1991 when it was established
- Act IDs are sequential and can be enumerated
- Search by year is the most reliable discovery method
