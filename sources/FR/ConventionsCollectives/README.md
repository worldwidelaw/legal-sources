# FR/ConventionsCollectives

French collective bargaining agreements (conventions collectives) from the DILA KALI database.

## Overview

Collective agreements are labor contracts negotiated between employer associations and
trade unions that define working conditions, salaries, benefits, and workplace rules
for specific industries or professions in France.

**IDCC** (Identifiant de la Convention Collective) is the unique code identifying each
collective agreement. For example:
- IDCC 3017: Syntec (IT, engineering, consulting)
- IDCC 1486: Bureaux d'études techniques
- IDCC 176: Pharmacie d'officine

## Data Source

- **URL**: https://echanges.dila.gouv.fr/OPENDATA/KALI/
- **Format**: XML archives (tar.gz)
- **Update frequency**: Daily
- **License**: Licence Ouverte / Open Licence (Etalab)

## Usage

```bash
# Fetch sample records (15 by default)
python bootstrap.py bootstrap --sample

# Full fetch with global dump (~173MB)
python bootstrap.py bootstrap --full

# Fetch updates since a date
python bootstrap.py updates --since 2026-02-01

# Check status
python bootstrap.py status
```

## Data Structure

The KALI database follows DILA's standard structure:
- **KALICONT**: Convention collective container (metadata, signatories, notes)
- **KALISCTA**: Section structure (titles, hierarchical organization)
- **KALIARTI**: Article content (full text of each article)

## Output Schema

Each record contains:
- `_id`: Document identifier (e.g., KALICONT000005635096)
- `_source`: "FR/ConventionsCollectives"
- `_type`: "collective_agreement"
- `idcc`: IDCC code
- `title`: Full title of the agreement
- `text`: Full text (combined articles)
- `date`: Signature date (YYYY-MM-DD)
- `url`: Légifrance URL
- `etat`: Status (EN_VIGUEUR, ABROGE, etc.)
- `num_brochure`: Brochure reference number

## Coverage

- ~500+ national collective agreements
- Regional and departmental variants
- Salary grids (avenants salariaux)
- Extension orders (arrêtés d'extension)
- Associated texts and amendments
