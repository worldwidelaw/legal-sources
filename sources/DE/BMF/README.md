# DE/BMF - German Federal Ministry of Finance Tax Circulars

## Source Information

- **Name**: BMF-Schreiben (Bundesministerium der Finanzen)
- **Country**: Germany (DE)
- **Language**: German (de)
- **Data Type**: Doctrine (administrative guidance)
- **URL**: https://www.bundesfinanzministerium.de/Web/DE/Service/Publikationen/BMF_Schreiben/bmf_schreiben.html

## Overview

BMF-Schreiben are administrative instructions (Verwaltungsanweisungen) issued by the German Federal Ministry of Finance. They provide binding guidance to tax authorities on how to interpret and apply tax law.

## Coverage

- **Documents**: 500+ BMF-Schreiben
- **Time Period**: Historical archive covering multiple years
- **Update Frequency**: New circulars published regularly

## Tax Areas Covered

- Income tax (Einkommensteuer)
- Corporate tax (Körperschaftsteuer)
- VAT (Umsatzsteuer)
- International tax law
- Tax procedure (Abgabenordnung)
- Inheritance and gift tax
- Real property tax

## Data Access Method

1. HTML listing pages scraped for document discovery
2. PDF downloads for full text
3. pypdf library used for text extraction

## Document Structure

Each document includes:
- **Title**: Subject of the circular
- **Date**: Publication date
- **GZ (Geschäftszeichen)**: Official reference number
- **DOK**: Internal document ID
- **Category**: Tax area (e.g., "Steuern")
- **Full Text**: Complete circular content

## License

Public Domain - BMF-Schreiben are "amtliche Werke" (official works) which are not protected by copyright under German law (§ 5 UrhG).

## Usage

```bash
# Test fetcher (3 documents)
python3 bootstrap.py

# Bootstrap with sample data (12 documents)
python3 bootstrap.py bootstrap --sample

# Full bootstrap (all documents)
python3 bootstrap.py bootstrap
```

## Dependencies

- requests
- beautifulsoup4
- pypdf (for PDF text extraction)

## Notes

- Rate limiting is applied (1.5s between requests)
- PDFs are downloaded and processed one at a time
- Text extraction quality depends on PDF structure
