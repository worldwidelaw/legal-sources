# CH/ESTV - Swiss Federal Tax Administration

Fetches tax circulars and administrative directives from the Swiss Federal Tax Administration (ESTV/AFC).

## Data Source

- **Provider**: Eidgenössische Steuerverwaltung (ESTV) / Administration fédérale des contributions (AFC)
- **URL**: https://www.estv.admin.ch
- **License**: Public domain (Swiss federal government)
- **Languages**: German (de), French (fr), Italian (it)

## Document Types

- **Kreisschreiben** (Tax circulars): Interpretive guidance on tax law
- **Rundschreiben** (Administrative notices): Internal directives and procedural guidance

## Tax Areas Covered

1. **Direkte Bundessteuer** (Direct Federal Tax)
2. **Verrechnungssteuer** (Withholding Tax)

## Data Collection Method

The ESTV website uses Nuxt.js with embedded JSON data. Documents are parsed from the page data and PDFs are downloaded from the backend file service.

## Usage

```bash
# Fetch sample records (15 documents)
python bootstrap.py bootstrap --sample

# Fetch all documents
python bootstrap.py bootstrap --full

# Fetch specific number of samples
python bootstrap.py bootstrap --sample --count 20
```

## Requirements

- Python 3.8+
- requests
- pdfplumber or pypdf (for PDF text extraction)

## Output Schema

Each record contains:
- `_id`: Unique identifier (CH/ESTV/{doc_id})
- `title`: Document title
- `text`: Full text extracted from PDF
- `date`: Publication date (ISO 8601)
- `url`: Link to PDF document
- `circular_type`: kreisschreiben or rundschreiben
- `circular_number`: Number if available (e.g., "50a")
- `tax_area`: direkte_bundessteuer or verrechnungssteuer
- `language`: de, fr, or it
