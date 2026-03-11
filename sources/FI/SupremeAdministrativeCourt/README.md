# FI/SupremeAdministrativeCourt

Finnish Supreme Administrative Court (Korkein hallinto-oikeus / KHO) case law.

## Data Source

**LawSampo Linked Open Data Service**
- SPARQL Endpoint: http://ldf.fi/lawsampo/sparql
- Official website: https://www.kho.fi

## Coverage

- 10,000+ KHO judgments
- Historical through 2021 (based on LawSampo data updates)
- Full text available in Finnish

## Data Access Method

Uses SPARQL queries to the LawSampo endpoint:
1. Query for all `lss:Judgment` records where `dcterms:creator` is KHO
2. Retrieve metadata (ECLI, date, judgment number) and HTML full text
3. Extract clean text from HTML using BeautifulSoup

## Key Fields

- `ecli`: European Case Law Identifier (e.g., ECLI:FI:KHO:2021:104)
- `judgment_number`: KHO yearbook number (e.g., 104)
- `date`: Decision date in ISO format
- `text`: Full judgment text in Finnish

## License

- CC BY 4.0 (LawSampo / Semantic Finlex)

## Notes

- The Finlex Open Data API requires authentication for case law access
- LawSampo provides the same data as Linked Open Data without authentication
- Data may not include most recent decisions (2022+) pending LawSampo updates
- Full text is extracted from HTML content stored in the `lss:html` property
