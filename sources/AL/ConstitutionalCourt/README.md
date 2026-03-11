# AL/ConstitutionalCourt

Albanian Constitutional Court (Gjykata Kushtetuese) case law fetcher.

## Data Source

- **Website**: https://www.gjykatakushtetuese.gov.al
- **API**: WordPress REST API (wp-json/wp/v2/)
- **Auth**: None (public)
- **Data Types**: case_law

## Records

- `court_decision`: ~6 recent court decisions with PDF attachments
- `kerkesa_vendimi`: ~4,600 decision request records
- Media library: ~1,700 PDF files

## Full Text

Full text is extracted from PDF attachments using pdfplumber or PyPDF2.

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample data (10 records)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Notes

- Language: Albanian (sq)
- EU candidate country
- Some PDFs may be scanned images (OCR not implemented)
