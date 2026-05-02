# INTL/EFTACourt — EFTA Court Judgments and Advisory Opinions

Judgments and advisory opinions from the EFTA Court, the judicial body
of the European Free Trade Association that interprets the EEA Agreement
for Iceland, Liechtenstein, and Norway.

- **Coverage:** ~455 cases from 1994 to present
- **Types:** Judgments, advisory opinions, orders
- **Language:** English (some French)
- **Update frequency:** As new cases are decided

## How it works

1. Paginates the WordPress REST API (`/wp-json/wp/v2/cases`) to list all cases
2. For each case, fetches the case page HTML to extract metadata and PDF download links
3. Downloads judgment PDFs and extracts full text via `common.pdf_extract`

## License

[EFTA Court Public Records](https://eftacourt.int/) — Court judgments are public judicial records published for open access. No restrictive terms of use found on the website. Attribution recommended.
