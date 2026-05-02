# NZ/CommerceCommission — New Zealand Commerce Commission Case Register

Competition, consumer protection, and regulatory enforcement decisions from the
New Zealand Commerce Commission. Covers mergers, anti-competitive conduct,
Fair Trading Act enforcement, Credit Contracts Act, and regulated industries
(telecommunications, electricity, gas, airports).

- **URL**: https://comcom.govt.nz/case-register
- **Coverage**: ~1,866 cases (2000s–present)
- **Data type**: case_law (decisions, determinations, clearances)
- **Language**: English
- **Auth**: None (open access)

## Strategy

1. Parse sitemap.xml to enumerate all case register entry URLs
2. Fetch each case page and extract structured metadata from HTML
3. Parse embedded Vue.js `<project-block>` JSON for document timeline
4. Identify and download decision PDFs from timeline
5. Extract full text from PDFs using common/pdf_extract

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Crown copyright,
licensed for reuse with attribution by the Commerce Commission on behalf of the
New Zealand Government.
