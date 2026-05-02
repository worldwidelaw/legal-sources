# ES/Balearics — Balearic Islands Regional Legislation (BOIB)

Regional legislation from the **Butlletí Oficial de les Illes Balears** (BOIB),
the official gazette of the Balearic Islands autonomous community.

## Data Source

- **URL**: https://www.caib.es/eboibfront/
- **Format**: ELI XML with full text in `<env:contingut>`
- **Coverage**: 2013–present (Section I: Disposicions Generals)
- **Language**: Catalan (primary), Spanish translations available
- **Document types**: Laws, Decrees, Ordinances, Regulations, Budgets

## Strategy

1. Crawl annual calendar pages to discover bulletin IDs
2. Parse each bulletin's table of contents for XML document links
3. Fetch XML via ELI endpoints — full text + metadata in one request
4. Filter for Section I (Disposicions Generals) = core legislation

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Attribution to Govern de les Illes Balears.
