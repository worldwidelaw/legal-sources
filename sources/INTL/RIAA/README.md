# INTL/RIAA — UN Reports of International Arbitral Awards

International arbitral awards between states published by the UN Codification Division. 34 volumes covering awards from the 18th century to present.

## Coverage

- **Type**: Case law (international arbitral awards)
- **Scope**: State-to-state and state-to-international organization disputes
- **Volumes**: I–XXXIV (~1190 individual awards)
- **Languages**: English and French
- **Period**: 18th century to 2018

## Strategy

1. Scrape each volume page (`vol_1.shtml` through `vol_34.shtml`) for case entries
2. Parse case title, parties, and dates from HTML table rows
3. Download individual case PDFs from `legal.un.org/riaa/cases/`
4. Extract full text via `common/pdf_extract`

## License

[United Nations Terms of Use](https://www.un.org/en/about-us/terms-of-use) — reproduction permitted with attribution.
