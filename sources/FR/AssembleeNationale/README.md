# FR/AssembleeNationale - French National Assembly

Data source for French National Assembly parliamentary documents.

## Overview

Fetches **projets de loi** (government bills) and **propositions de loi** (member bills)
from the French National Assembly's Open Data portal with full text content.

## Data Sources

1. **Document Catalog** (ZIP): `data.assemblee-nationale.fr/static/openData/repository/17/loi/dossiers_legislatifs/`
   - Contains document IDs and metadata in JSON format
   - Updated daily
   - Covers legislature 17 (current) and archives

2. **Full Text** (HTML): `www.assemblee-nationale.fr/dyn/docs/{doc_id}.raw`
   - Rendered HTML with full bill/law text
   - Includes exposé des motifs, articles, signatures

## Document Types

- **PRJL**: Projets de loi (government bills)
- **PION**: Propositions de loi (member bills)
- **RAPP**: Rapports (committee reports)
- **PNRE**: Propositions de résolution (resolutions)

## Schema

| Field | Description |
|-------|-------------|
| _id | Document UID (e.g., PRJLANR5L17B0621) |
| _source | FR/AssembleeNationale |
| _type | legislation |
| title | Document title |
| text | Full text content |
| date | Deposit date (YYYY-MM-DD) |
| url | Link to document on assemblee-nationale.fr |
| legislature | Legislature number (e.g., 17) |
| denomination | Document type (Projet de loi, etc.) |

## License

Data is provided under the French Open License (Licence Ouverte / Open Licence).

## Usage

```bash
# Fetch sample records
python bootstrap.py bootstrap --sample

# Fetch all records
python bootstrap.py bootstrap --full

# Fetch updates since a date
python bootstrap.py updates --since 2024-01-01
```
