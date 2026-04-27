# BE/APD - Belgian Data Protection Authority

Belgian Data Protection Authority (Autorité de protection des données / Gegevensbeschermingsautoriteit) GDPR enforcement decisions.

## Data Source

- French: https://www.autoriteprotectiondonnees.be/citoyen/publications/decisions
- Dutch: https://www.gegevensbeschermingsautoriteit.be/burger/publicaties/beslissingen

## Data Types

- **case_law**: GDPR enforcement decisions (sanctions, warnings, dismissals, orders)

## Coverage

- ~1200 decisions total (2018-present)
- Decisions in both French and Dutch
- Decision types:
  - Substantive decisions (décisions au fond / beslissingen ten gronde)
  - Dismissals (classements sans suite / zonder gevolg)
  - Warnings (avertissements / waarschuwingen)
  - Orders (ordonnances / bevelen)
  - Court judgments (arrêts / arresten)

## Access Method

HTML scraping + PDF text extraction:
1. Scrape decision listing pages
2. Extract PDF URLs from listing
3. Download PDFs and extract text using pypdf

## License

[Belgian Open Government Data](https://data.gov.be/en/licence-conditions) — free reuse of Belgian public sector information.
