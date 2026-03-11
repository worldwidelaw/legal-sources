# LU/LegalDatabase — Luxembourg Legal Database (Legilux)

Official Luxembourg legislation from the [Legilux portal](https://legilux.public.lu), managed by the Service central de législation (Ministry of State).

## Data Source

- **Portal**: https://legilux.public.lu
- **SPARQL Endpoint**: https://data.legilux.public.lu/sparqlendpoint
- **Documentation**: https://data.legilux.public.lu/
- **License**: CC BY 4.0

## Coverage

| Document Type | Code | Count |
|--------------|------|-------|
| Laws | LOI | 9,000+ |
| Grand-Ducal Regulations | RGD | 15,000+ |
| Grand-Ducal Decrees | AGD | 9,000+ |
| Ministerial Orders | AMIN | 12,000+ |
| Ministerial Regulations | RMIN | 6,000+ |
| Decrees | A | 7,000+ |

Total: ~150,000+ legal acts

## Technical Details

### Data Model

Luxembourg uses the FRBR (Functional Requirements for Bibliographic Records) model:
- **Work** (Act) → **Expression** → **Manifestation** → **File**

All resources have ELI (European Legislation Identifier) URIs.

### Access Method

1. SPARQL query to get metadata + HTML file URLs
2. Fetch HTML full text from the filestore
3. Extract clean text from HTML

### ELI URI Pattern

```
http://data.legilux.public.lu/eli/etat/leg/{type}/{yyyy}/{mm}/{dd}/{id}/jo
```

Example:
```
http://data.legilux.public.lu/eli/etat/leg/loi/2026/02/05/a29/jo
```

## Usage

```bash
# Test connection
python bootstrap.py test

# Fetch sample (10+ records)
python bootstrap.py bootstrap --sample

# Full bootstrap (all documents)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Sample Output

```json
{
  "_id": "etat/leg/loi/2026/02/05/a29/jo",
  "_source": "LU/LegalDatabase",
  "_type": "legislation",
  "title": "Loi du 5 février 2026 autorisant le Gouvernement...",
  "text": "Nous Guillaume, Grand-Duc de Luxembourg...",
  "date": "2026-02-05",
  "url": "http://data.legilux.public.lu/eli/etat/leg/loi/2026/02/05/a29/jo",
  "document_type": "LOI",
  "is_in_force": true,
  "language": "fr"
}
```

## Notes

- Luxembourg chairs the EU ELI Task Force — data quality is excellent
- Primary language is French, some documents in German
- Full text extracted from HTML manifestations
- In-force status tracked in metadata
