# EU/CURIA - Court of Justice of the European Union

## Overview

This data source provides access to case law from the Court of Justice of the European Union (CJEU), including:

- **Court of Justice** (CJ) - The highest court, handles preliminary rulings, actions for annulment, etc.
- **General Court** (TJ) - First instance court for actions by individuals and companies
- **Civil Service Tribunal** (FJ) - Historical cases related to EU staff (merged into General Court in 2016)

## Data Access Method

1. **Discovery**: SPARQL endpoint at `http://publications.europa.eu/webapi/rdf/sparql`
   - Queries the EU Publications Office Cellar database
   - Returns CELEX identifiers, dates, ECLI numbers, and titles

2. **Full Text Retrieval**: EUR-Lex HTML endpoint
   - URL pattern: `https://eur-lex.europa.eu/legal-content/EN/TXT/HTML/?uri=CELEX:{celex}`
   - Returns full judgment text in HTML format

## Document Identifiers

### CELEX Numbers

CELEX sector 6 format: `6{year}{court}{number}`

- **6** = Case law sector
- **year** = 4-digit year of case registration
- **court** = Two-letter court code:
  - `CJ` = Court of Justice judgment
  - `TJ` = General Court judgment
  - `FJ` = Civil Service Tribunal judgment
  - `CO` = Court of Justice order
  - `CC` = Opinion of Advocate General
- **number** = Sequential case number

Examples:
- `62023CJ0001` = Court of Justice Judgment, Case C-1/23
- `62022TJ0100` = General Court Judgment, Case T-100/22

### ECLI (European Case Law Identifier)

Format: `ECLI:EU:{court}:{year}:{number}`

Example: `ECLI:EU:C:2023:1`

## Document Types

| Type | Description |
|------|-------------|
| JUDG | Judgments from all courts |
| ORDER | Orders (procedural decisions) |
| OPIN_AG | Opinions of Advocates General |
| OPIN_CJ | Opinions of the Court on international agreements |

## Usage

```bash
# Fetch sample data (12 documents)
python3 bootstrap.py bootstrap --sample

# Fetch larger sample (50 documents)
python3 bootstrap.py bootstrap

# Test mode (5 documents, prints to console)
python3 bootstrap.py
```

## Output Schema

```json
{
  "_id": "62023CJ0001",
  "_source": "EU/CURIA",
  "_type": "case_law",
  "_fetched_at": "2026-02-09T12:00:00",
  "celex_id": "62023CJ0001",
  "ecli": "ECLI:EU:C:2023:1",
  "court": "Court of Justice",
  "document_type": "judgment",
  "title": "Judgment of the Court...",
  "text": "Full text of the judgment...",
  "date": "2023-04-18",
  "url": "https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:62023CJ0001"
}
```

## Rate Limiting

- 1.5 second delay between document requests
- SPARQL queries batched (50 documents per query)

## License

[EUR-Lex legal notice](https://eur-lex.europa.eu/content/legal-notice/legal-notice.html) — reuse authorised provided the source is acknowledged.

## Related Sources

- **EU/EUR-Lex**: Also contains CJEU case law plus EU legislation
- CURIA and EUR-Lex share the same underlying database (Cellar)
