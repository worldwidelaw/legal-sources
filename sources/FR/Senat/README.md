# FR/Senat - French Senate (Sénat)

Legislative documents from the French Senate in Akoma Ntoso XML format.

## Data Source

- **Portal**: https://data.senat.fr
- **Documents**: https://www.senat.fr/akomantoso/
- **Coverage**: December 1, 2019 onwards
- **Format**: Akoma Ntoso 3.0 XML
- **License**: Licence Ouverte / Open Licence Etalab

## API Access

Documents are indexed via two XML files:
1. **Deposited texts**: `https://www.senat.fr/akomantoso/depots.xml`
2. **Adopted texts**: `https://www.senat.fr/akomantoso/adoptions.xml`

Individual documents are accessed via their specific URLs:
- `https://www.senat.fr/akomantoso/{signet}.akn.xml`

## Document Types

| Prefix | Type | Description |
|--------|------|-------------|
| `pjl` | Projet de loi | Government bill |
| `ppl` | Proposition de loi | Private member bill |
| `ppr` | Proposition de résolution | Resolution proposal |

## Document Structure

Akoma Ntoso XML documents contain:
- **meta/identification**: FRBR identifiers (FRBRWork, FRBRExpression)
- **meta/workflow**: Legislative process steps
- **preamble/docTitle**: Document title
- **body**: Full text organized in articles, alineas, and paragraphs

## Key Fields

| Field | Description |
|-------|-------------|
| `FRBRthis` | FRBR Work identifier |
| `FRBRuri` | FRBR URI |
| `docTitle` | Document title |
| `signet` | Senate reference ID |
| `FRBRdate[@name='depot']` | Filing date |
| `FRBRdate[@name='adoption']` | Adoption date |

## Usage

```bash
# Fetch sample records
python bootstrap.py bootstrap --sample

# Fetch all records
python bootstrap.py bootstrap --full

# Fetch updates since a date
python bootstrap.py updates --since 2024-01-01
```

## Technical Notes

- Akoma Ntoso is the international standard for legislative documents (OASIS LegalDocumentML)
- Full text is extracted from `body/article/alinea/content/p` elements
- Documents include FRBR (Functional Requirements for Bibliographic Records) identifiers

## License

[Licence Ouverte 2.0 / Open Licence (Etalab)](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) — free reuse with attribution.
