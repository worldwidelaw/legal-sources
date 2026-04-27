# KE/CAK — Competition Authority of Kenya

Determinations from the Competition Authority of Kenya (CAK), covering merger
decisions, abuse of dominance investigations, consumer protection cases, and
exemption applications.

- **Source**: https://www.cak.go.ke/information-center/CAK-latest-determinations
- **Format**: PDF determinations served from Drupal CMS
- **Coverage**: ~100+ determinations from 2014 to present
- **Language**: English
- **Data types**: case_law, doctrine

## Strategy

1. Scrape paginated listing pages (?page=0 through ?page=6)
2. Extract PDF download links from each page
3. Download and extract full text from each PDF
4. Normalize into standard schema

## License

[Kenya Government Open Data](https://www.cak.go.ke/) — official regulatory decisions published for public access. Attribution required.
