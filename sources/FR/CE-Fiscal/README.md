# FR/CE-Fiscal — French Council of State Tax Chamber Decisions

Fiscal/tax case law from the Conseil d'État (Supreme Administrative Court of France).

## Data Source

Same open data archives as FR/CouncilState (`opendata.justice-administrative.fr`), filtered to fiscal matters.

## Filtering Logic

Decisions are identified as fiscal by two criteria (OR logic):

1. **Chamber assignment**: Decisions from known fiscal chambers (8ème, 9ème, 3ème+8ème, 9ème+10ème)
2. **Content keywords**: Decisions containing 2+ fiscal legal terms (e.g., "code général des impôts", "livre des procédures fiscales")

## Usage

```bash
python bootstrap.py bootstrap --sample          # 15 fiscal samples
python bootstrap.py bootstrap --sample --count 30  # 30 samples
python bootstrap.py bootstrap --full             # All fiscal decisions
python bootstrap.py updates --since 2025-01-01   # Incremental
```

## Coverage

- **Period**: September 2021 – present
- **Court**: Conseil d'État only (tier 1)
- **Type**: case_law (fiscal/tax)
- **Format**: XML with full text
- **License**: Licence Ouverte / Open Licence
