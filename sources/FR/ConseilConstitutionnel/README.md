# FR/ConseilConstitutionnel - French Constitutional Council

## Overview

This source fetches decisions from the French Constitutional Council (Conseil constitutionnel) via DILA's CONSTIT open data.

## Data Source

- **Endpoint**: https://echanges.dila.gouv.fr/OPENDATA/CONSTIT/
- **Format**: XML (tar.gz archives)
- **License**: Open Licence 2.0 (Licence Ouverte)
- **Coverage**: All decisions since 1958

## Decision Types

- **DC** - Décision de conformité (Constitutional review of laws before promulgation)
- **QPC** - Question Prioritaire de Constitutionnalité (Priority constitutional questions, since 2010)
- **AN** - Assemblée Nationale (National Assembly electoral disputes)
- **SEN** - Sénat (Senate electoral disputes)
- **PDR** - Président de la République (Presidential election disputes)
- **REF** - Référendum (Referendum decisions)

## Schema

Each normalized record contains:

| Field | Description |
|-------|-------------|
| _id | Unique document ID (CONSTEXT...) |
| _source | "FR/ConseilConstitutionnel" |
| _type | "case_law" |
| _fetched_at | ISO timestamp |
| title | Decision title |
| text | Full text of the decision |
| date | Decision date (YYYY-MM-DD) |
| url | Link to decision on conseil-constitutionnel.fr |
| ecli | ECLI identifier (ECLI:FR:CC:YYYY:...) |
| numero | Decision number (e.g., "2025-1140") |
| nature | Decision type code |
| nature_qualifiee | Full decision type |
| solution | Outcome (conformité, non-conformité, etc.) |
| juridiction | "Conseil constitutionnel" |
| nor | NOR identifier for Journal Officiel |
| titre_jo | Journal Officiel publication reference |

## Usage

```bash
# Generate sample data (100 decisions)
python3 bootstrap.py bootstrap --sample --count 100

# Full bootstrap (all decisions)
python3 bootstrap.py bootstrap

# Fetch updates since a date
python3 bootstrap.py updates --since 2026-01-01
```

## Sample Statistics

- Total samples: 100
- Average text length: ~3,300 chars
- Decision types in sample: Electoral (AN)
- Full archive contains ~8,000+ decisions including DC and QPC

## Notes

- The full archive (Freemium_constit_global_*.tar.gz) is ~12MB
- Incremental archives are published quarterly
- Full text includes the complete decision with reasoning (considérants)
- Personal names are anonymized in some decisions

## License

[Licence Ouverte 2.0 / Open Licence (Etalab)](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) — free reuse with attribution.
