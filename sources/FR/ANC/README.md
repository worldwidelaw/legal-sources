# FR/ANC - Autorité des Normes Comptables

French Accounting Standards Authority data source.

## Data Source

- **Website**: https://www.anc.gouv.fr
- **Regulations**: https://www.anc.gouv.fr/normes-comptables-francaises/reglements-de-lanc
- **License**: Open Licence Etalab

## Content

- **Règlements ANC**: French accounting regulations (GAAP)
- **Recueils**: Consolidated accounting standards with doctrine annotations
- **Avis**: ANC opinions on accounting matters

## Technical Details

- Regulations are published as PDFs
- Two versions typically available:
  - JO (Journal Officiel): Official published version
  - Recueil: Annotated version with doctrine commentary
- Text extracted using pypdf library

## Usage

```bash
# Fetch sample records
python bootstrap.py bootstrap --sample

# Full fetch
python bootstrap.py bootstrap --full
```

## Notes

- Regulations from 2022 onwards use the new website structure
- Older regulations (2014-2021) are available on an archived page
- The Plan Comptable Général (PCG) is the main accounting framework
