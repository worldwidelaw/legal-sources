# CH/Fedlex - Swiss Federal Legislation

Swiss Federal Chancellery legislation database via Fedlex SPARQL endpoint.

## Data Source

- **Portal**: https://www.fedlex.admin.ch
- **SPARQL Endpoint**: https://fedlex.data.admin.ch/sparqlendpoint
- **Data Model**: JOLux ontology (FRBR-based with ELI URIs)
- **License**: Open Government Data (OGD Switzerland)

## Coverage

- **Official Compilation (OC)**: Federal laws as published
- **Official Federal Gazette (FGA)**: Federal notices and announcements
- **Classified Compilation (CC)**: Current consolidated law
- **Languages**: German (DE), French (FR), Italian (IT), Romansh (RM)
- **Total Acts**: ~209,000+

## Document Types

- Federal Constitution
- Federal Acts (Bundesgesetz)
- Ordinances (Verordnung)
- Decrees (Beschluss)
- International treaties
- Notices and announcements

## API Details

Uses SPARQL with JOLux ontology for:
1. Discovering acts via `jolux:Act` type
2. Getting language expressions via `jolux:isRealizedBy`
3. Getting file manifestations via `jolux:isEmbodiedBy`
4. Downloading full text from `jolux:isExemplifiedBy` URLs

Available formats: HTML, XML, PDF, DOCX

## Usage

```bash
# Fetch sample documents
python3 bootstrap.py bootstrap --sample --count 12

# Fetch updates from last 7 days
python3 bootstrap.py update --days 7
```

## Rate Limiting

- 0.5 second delay between requests
- SPARQL queries may timeout for large result sets

## License

[OGD Switzerland](https://opendata.swiss/en/terms-of-use) — Open Government Data.

## Notes

- Switzerland is not an EU member but is part of EFTA
- Uses European Legislation Identifier (ELI) standard
- All ELI URIs start with: `https://fedlex.data.admin.ch/eli/`
