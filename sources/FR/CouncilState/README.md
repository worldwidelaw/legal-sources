# FR/CouncilState - French Council of State (Conseil d'État)

Administrative Supreme Court case law and decisions from the French Conseil d'État.

## Data Source

- **Portal**: https://opendata.justice-administrative.fr
- **Coverage**: Decisions from September 30, 2021 onwards
- **Format**: XML with full text
- **License**: Licence Ouverte / Open Licence (Etalab)

## API Access

Decisions are available via:
1. **Monthly ZIP archives**: `https://opendata.justice-administrative.fr/DCE/{year}/{month}/CE_{yearmonth}.zip`
2. **Search API**: `https://opendata.justice-administrative.fr/recherche/api/model_search_juri/openData/CE/{query}/{limit}`

This implementation uses the ZIP archives as they contain the full XML with complete decision text.

## Document Structure

Each XML document contains:
- `Donnees_Techniques`: Technical metadata (filename, update date)
- `Dossier`: Case information (ECLI, case number, court, decision type, etc.)
- `Audience`: Hearing information (date, formation)
- `Decision/Texte_Integral`: Full text of the decision

## Key Fields

| Field | Description |
|-------|-------------|
| `Numero_ECLI` | European Case Law Identifier |
| `Numero_Dossier` | Case number |
| `Date_Lecture` | Decision date |
| `Type_Decision` | Decision type (Décision, Ordonnance, etc.) |
| `Type_Recours` | Appeal type |
| `Code_Publication` | Publication classification (A, B, C, D, Z) |
| `Solution` | Outcome (Rejet, Annulation, etc.) |
| `Formation_Jugement` | Court formation |
| `Texte_Integral` | Full decision text |

## Publication Codes

- **A**: Published in the Recueil Lebon
- **B**: Published in the Tables du Recueil Lebon
- **C**: Mentionnable (notable)
- **D**: Not published
- **Z**: Other

## Usage

```bash
# Fetch sample records
python bootstrap.py bootstrap --sample

# Fetch all records
python bootstrap.py bootstrap --full

# Fetch updates since a date
python bootstrap.py updates --since 2024-01-01
```

## Legal Framework

Open data access is mandated by French law n°2019-222 of March 23, 2019 (Justice Reform Act), which requires progressive publication of all court decisions in open data format.
