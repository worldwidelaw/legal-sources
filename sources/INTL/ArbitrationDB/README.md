# INTL/ArbitrationDB — International Arbitration Database

**Source:** [The International Arbitration Society](https://arbitration.org/)

Free online database of international arbitration awards, established in 2008.
Covers ICSID, UNCITRAL, ICC, PCA, and other investment/commercial arbitration
proceedings. Contains ~8,500 arbitral awards with downloadable PDF documents.

## Data

- **Type:** case_law (arbitral awards and decisions)
- **Coverage:** International investment and commercial arbitration
- **Format:** HTML metadata + PDF full text
- **Language:** Primarily English, some French/Spanish

## Strategy

1. Scrape paginated listing at `/award/recent?page=N` (943+ pages, 9 per page)
2. Scrape each award detail page at `/award/{ID}` for metadata and PDF links
3. Download PDFs and extract text via `common/pdf_extract`

## License

[Custom Terms](https://arbitration.org/node/32) — Free public database of arbitral awards.
Terms of use page is a placeholder. Awards are public decisions of international tribunals.
