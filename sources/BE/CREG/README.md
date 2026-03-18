# BE/CREG - Belgian Energy Regulator

## Data Source

**CREG** (Commission de Régulation de l'Électricité et du Gaz) is the Belgian federal regulator for electricity and natural gas markets.

- **Website**: https://www.creg.be
- **Publications**: https://www.creg.be/fr/publications

## Data Types

- **Decisions** (Décisions) - Regulatory decisions on tariffs, market operations, certification
- **Studies** (Études) - Market analysis and research publications
- **Notes** - Briefing notes and commentary
- **Reports** (Rapports) - Annual reports and compliance reports

## Access Method

HTML scraping of Drupal-based website. No API available.
PDFs are downloaded and text is extracted using pdfplumber.

## Usage

```bash
# Fetch sample (15 records)
python bootstrap.py bootstrap --sample

# Fetch all decisions
python bootstrap.py bootstrap --full

# Fetch custom sample count
python bootstrap.py bootstrap --sample --count 20
```

## Schema

Key fields in normalized records:
- `_id`: Unique identifier (e.g., "BE/CREG/B3164")
- `title`: Publication title
- `text`: Full text extracted from PDF
- `date`: Approval/publication date
- `reference_number`: CREG reference (e.g., "Décision (B)3164")
- `summary`: Abstract/summary from webpage
- `themes`: Subject categories
- `pdf_url`: Direct link to PDF document

## Notes

- Publications available in French, Dutch, and some English
- Decision reference numbers follow pattern: (B)XXXX for decisions, (F)XXXX for studies
- Rate limiting: 2 second delay between requests
