# SE/Domstolverket - Swedish Courts Case Law

Case law from all Swedish courts via Domstolsverket's Open Data API. This source covers courts of appeal (hovrätter), administrative courts of appeal (kammarrätter), and specialized courts.

## Coverage

This source complements:
- **SE/SupremeCourt** - Högsta domstolen (HDO) - Supreme Court
- **SE/SupremeAdministrativeCourt** - Högsta förvaltningsdomstolen (HFD)

Courts included in this source:

### Courts of Appeal (Hovrätter)
- HSV: Svea hovrätt
- HGO: Göta hovrätt
- HVS: Hovrätten för Västra Sverige
- HON: Hovrätten för Övre Norrland
- HNN: Hovrätten för Nedre Norrland
- HSB: Hovrätten över Skåne och Blekinge

### Administrative Courts of Appeal (Kammarrätter)
- KST: Kammarrätten i Stockholm
- KGG: Kammarrätten i Göteborg
- KJO: Kammarrätten i Jönköping
- KSU: Kammarrätten i Sundsvall

### Specialized Courts
- ADO: Arbetsdomstolen (Labour Court)
- MMOD: Mark- och miljööverdomstolen (Land & Environment Court of Appeal)
- MIOD: Migrationsöverdomstolen (Migration Court of Appeal)
- PMOD: Patent- och marknadsöverdomstolen (Patent & Market Court of Appeal)

## Data Access

Uses Domstolsverket's REST API:
- API base: `https://rattspraxis.etjanst.domstol.se/api/v1`
- API docs: `https://rattspraxis.etjanst.domstol.se/openapi/puh-openapi.yaml`
- No authentication required

## Usage

```bash
# List available courts
python3 bootstrap.py courts

# Test API connection
python3 bootstrap.py test

# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Include supreme courts in fetch (for completeness)
python3 bootstrap.py bootstrap --sample --include-supreme
```

## Sample Record

```json
{
  "_id": "abc123-xyz",
  "_source": "SE/Domstolverket",
  "_type": "case_law",
  "title": "Case regarding employment dispute (T 1234-25)",
  "text": "SVEA HOVRÄTT DOM...",
  "date": "2025-12-15",
  "court": "Svea hovrätt",
  "court_code": "HSV",
  "case_numbers": ["T 1234-25"],
  "is_precedent": false,
  "keywords": ["employment", "termination"]
}
```

## License

Public Domain - Swedish court decisions are public documents (allmänna handlingar) under the Swedish Freedom of the Press Act (Tryckfrihetsförordningen).

## Notes

- Full text is extracted from PDF attachments using pdfplumber
- Some decisions have only summaries available
- Metadata includes SFS references (legal provisions)
- Rate limited to 1 request/second
