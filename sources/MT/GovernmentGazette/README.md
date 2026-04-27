# MT/GovernmentGazette - Malta Official Legislation

Data source for Malta's consolidated legislation from [legislation.mt](https://legislation.mt).

## Overview

- **Country:** Malta (MT)
- **Data Types:** Legislation (primary, constitutional, subsidiary)
- **License:** Open Government Data
- **Authentication:** None required

## Data Access Strategy

Malta implements the ELI (European Legislation Identifier) standard. However, full text is only available in PDF format (no HTML/XML alternative as of 2026).

### Endpoints Used

1. **ELI Page**: `https://legislation.mt/eli/cap/{number}/eng`
   - Returns HTML page with JSON-LD metadata
   - Contains embedded PDF viewer with `getpdf/{id}` URL

2. **PDF Download**: `https://legislation.mt/getpdf/{pdf_id}`
   - Returns PDF document
   - Text extracted using pdfplumber

### Document Types

- `eli/const` - Constitution of Malta
- `eli/cap/{number}` - Primary legislation (Chapters 1-600+)
- `eli/sl/{chapter}.{number}` - Subsidiary legislation
- `eli/act/{year}/{number}` - Acts
- `eli/ln/{year}/{number}` - Legal notices

## Usage

```bash
# Quick connectivity test
python bootstrap.py test

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all chapters)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Dependencies

- `pdfplumber` - PDF text extraction

## Sample Output

```json
{
  "_id": "eli_cap_1",
  "_source": "MT/GovernmentGazette",
  "_type": "legislation",
  "title": "Ecclesiastical Courts (Constitution and Jurisdiction) Law",
  "text": "CHAPTER 1\nECCLESIASTICAL COURTS...",
  "date": "1995-10-01",
  "eli_identifier": "eli/cap/1",
  "chapter_number": 1,
  "in_force": true
}
```

## License

[Open Government Data](https://data.gov.mt) — freely reusable under Malta's open data policy.

## Notes

- Chapters are numbered 1-600+ (with gaps for repealed legislation)
- Full text extraction relies on PDF quality; some older documents may have OCR issues
- Constitution is accessed via `eli/const/eng`
