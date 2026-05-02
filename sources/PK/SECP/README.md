# PK/SECP — Pakistan Securities and Exchange Commission

Regulatory documents from SECP including acts, ordinances, rules, regulations,
directives, guidelines, and circulars (~580 documents, 1999-2026).

## Data Source

- **URL**: https://www.secp.gov.pk/laws/
- **Format**: HTML listing pages + PDF downloads (WordPress Download Manager)
- **Language**: English
- **Coverage**: Securities, corporate governance, insurance, Islamic finance, AML

## Method

1. Scrape HTML table listings from `/laws/{category}/` pages
2. Parse date, title, and wpdmdl download URL from each row
3. Download PDFs and extract text using `common.pdf_extract`
4. Normalize into standard schema

## License

[Pakistan Government Publication](https://www.secp.gov.pk/) — official regulatory
documents published for public compliance. No explicit license; government publications
are generally public domain under Pakistani law.
