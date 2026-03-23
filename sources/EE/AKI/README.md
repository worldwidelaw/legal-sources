# EE/AKI - Estonian Data Protection Authority

## Source Information

- **Name**: Andmekaitse Inspektsioon (Estonian Data Protection Authority)
- **Country**: Estonia (EE)
- **URL**: https://www.aki.ee
- **Data Types**: Regulatory decisions (GDPR enforcement)
- **Language**: Estonian

## Decision Categories

1. **Ettekirjutused** (Orders/Directives)
   - Enforcement orders issued to organizations
   - Published after becoming legally final
   - ~50+ documents available

2. **Vaideotsused** (Appeal Decisions)
   - Decisions on administrative appeals
   - Related to public information and data protection
   - ~80+ documents available

3. **Seisukohad** (Positions/Statements)
   - Official positions and guidance
   - Clarifications on data protection matters
   - ~10+ documents available

## Data Access

- **Method**: HTML scraping + PDF text extraction
- **Authentication**: None required (public access)
- **Rate Limit**: 1 request/second

## Dependencies

- `pdfplumber` (preferred) or `pypdf` for PDF text extraction
- `beautifulsoup4` for HTML parsing

## Usage

```bash
# Test connectivity
python bootstrap.py test-api

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Notes

- All decisions are in Estonian
- Full text is extracted from PDF documents
- Historical decisions back to ~2019 are available
- Some older decisions may only be available via information requests
