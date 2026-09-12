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
- **Classified Compilation (CC)**: current consolidated law — ~6,000 acts in
  force, crawled at article level (~300k article records in DE+FR)
- **Languages**: German (DE), French (FR), Italian (IT), Romansh (RM), English (EN);
  DE+FR are fetched by default (`--langs`)
- **Total Acts**: ~209,000+ across all compilations

## Document Types

- Federal Constitution
- Federal Acts (Bundesgesetz)
- Ordinances (Verordnung)
- Decrees (Beschluss)
- International treaties
- Notices and announcements

## Record granularity

The Classified Compilation is emitted **one record per article**:

| field | example |
|-------|---------|
| `_id` | `cc/220/de/art_814` |
| `sr_number` | `220` |
| `abbreviation` | `OR` |
| `article` | `814` |
| `url` | `https://www.fedlex.admin.ch/eli/cc/27/317_321_377/20260101/de#art_814` |
| `corpus` | `classified_compilation` |

Each article's `text` starts with the act title, SR number, consolidation
date and the enclosing book/title/chapter path, so a citation such as
"OR Art. 814" or "ZGB Art. 14" resolves to exactly one document
(issue #1615). Acts with no `<article>` markup — most short treaties —
are emitted as a single whole-act record (`corpus: classified_compilation`,
empty `article`). Federal Gazette acts keep `corpus: federal_gazette`.

`date` is the act's enactment date (`jolux:dateDocument`);
`consolidation_date` is the version ("Stand") actually stored.

## API Details

Uses SPARQL with JOLux ontology for:
1. Listing the Classified Compilation via `jolux:ConsolidationAbstract` +
   the `id-systematique` notation (the SR number), dropping acts with a
   `jolux:dateNoLongerInForce` in the past
2. Picking the newest `jolux:Consolidation` whose `jolux:dateApplicability`
   is not in the future — Fedlex publishes versions that enter force years ahead
3. Getting language expressions via `jolux:isRealizedBy`
4. Getting file manifestations via `jolux:isEmbodiedBy` and downloading the
   HTML from `jolux:isExemplifiedBy`

Available formats: HTML, XML, PDF, DOCX

The filestore serves HTML without a charset parameter, so the fetcher forces
UTF-8; trusting the `requests` default mojibakes every umlaut.

## Usage

```bash
# Fetch sample documents (a few articles from BV, ZGB, OR, StGB, StPO, DBG)
python3 bootstrap.py bootstrap --sample --count 24

# Full corpus -> data/records.jsonl
python3 bootstrap.py bootstrap --full
python3 bootstrap.py bootstrap --langs de,fr,it

# Acts whose consolidated text changed in the last 30 days
python3 bootstrap.py update --days 30
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
