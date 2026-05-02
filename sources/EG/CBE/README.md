# EG/CBE — Central Bank of Egypt Circulars

Regulatory circulars issued by the Central Bank of Egypt (CBE) to the banking
sector. Covers banking practices, prudential regulations, credit granting,
Basel regulation, consumer protection, payment regulation, and more.

- **Source:** https://www.cbe.org.eg/en/laws-regulations/regulations/circulars
- **Data type:** doctrine
- **Language:** Arabic (titles available in English via API)
- **Records:** ~388 circulars (2020–present)
- **Format:** JSON API for metadata + PDF downloads for full text

## Strategy

1. Paginated JSON API at `/api/listing/circulars?pageNo=N` returns 10 results per page
2. Each result includes title, date, categories, and a PDF URL
3. PDFs are downloaded and text extracted via pdfplumber
4. Content is primarily in Arabic

## License

[Open Government Data](https://www.cbe.org.eg/en/laws-regulations/regulations/circulars) — publicly published regulatory circulars, attribution required.
