# FR/ServicePublic - Service-Public.fr Fiches Pratiques

French administrative doctrine in the form of practical guides for individuals, businesses, and associations.

## Data Source

**Publisher:** Direction de l'information légale et administrative (DILA)

**URLs:**
- Particuliers + Associations: https://lecomarquage.service-public.gouv.fr/vdd/3.4/part/zip/vosdroits-latest.zip
- Entreprises: https://lecomarquage.service-public.gouv.fr/vdd/3.4/pro/zip/vosdroits-latest.zip

**Format:** ZIP containing XML files (schema version 3.4)

**License:** Licence Ouverte v2.0 (Open License)

**Update Frequency:** Daily

## Content Types

| Type | Description |
|------|-------------|
| Fiche d'information | Main practical information sheets |
| Fiche Question-réponse | Q&A format guides |
| Dossier | Topic bundles containing multiple fiches |
| CommentFaireSi | "How to" situational guides |

## Document Coverage

- **Particuliers:** ~5,500 documents covering individual citizens' rights and procedures
- **Entreprises:** ~2,500 documents for business-related procedures
- **Associations:** Included in Particuliers dataset

## Schema

Normalized records include:

| Field | Description |
|-------|-------------|
| `_id` | Document ID (e.g., F1729, N451) |
| `_source` | "FR/ServicePublic" |
| `_type` | "doctrine" |
| `title` | Document title |
| `text` | Full extracted text content |
| `date` | Last modification date |
| `url` | Link to service-public.fr |
| `audience` | Target audience (Particuliers/Entreprises) |
| `theme` | Main theme/category |
| `subject` | Subject area |
| `doc_type` | Document type |

## Usage

```bash
# Generate sample data (15 records)
python3 bootstrap.py bootstrap --sample

# Fetch all documents (outputs JSONL)
python3 bootstrap.py fetch

# Fetch updates since date
python3 bootstrap.py updates --since 2026-01-01

# Fetch specific audience only
python3 bootstrap.py fetch --audience particuliers
```

## License

[Licence Ouverte 2.0 / Open Licence (Etalab)](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) — free reuse with attribution.

## Attribution

When reusing this data, cite:
- Source: Service-Public.gouv.fr / DILA
- Download URL: lecomarquage.service-public.gouv.fr
