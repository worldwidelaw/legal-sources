# FR/AutoriteConcurrence - French Competition Authority

## Overview

This source fetches competition law decisions from the **Autorité de la Concurrence** (French Competition Authority), which also includes decisions from its predecessor, the Conseil de la Concurrence.

## Data Source

- **Portal**: [data.gouv.fr](https://www.data.gouv.fr/datasets/decisions-publiees-par-lautorite-de-la-concurrence-depuis-1988/)
- **Format**: JSON (full text)
- **License**: Open Licence 2.0 (Etalab)
- **Update frequency**: Monthly

## Coverage

- **Time period**: 1988 - present
- **Total decisions**: 6,500+
- **Decision types**:
  - **DCC** - Merger control decisions (Décisions de Contrôle des Concentrations)
  - **D** - Antitrust decisions (Décisions contentieuses)
  - **A** - Opinions (Avis)
  - **MC** - Interim measures (Mesures conservatoires)

## Usage

```bash
# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Show dataset statistics
python3 bootstrap.py stats
```

## Data Schema

Each decision includes:

| Field | Description |
|-------|-------------|
| `decision_id` | Unique identifier (e.g., "26-DCC-27") |
| `title` | Decision title |
| `date` | Decision date (ISO 8601) |
| `text` | Full text of the decision |
| `decision_type` | Type of decision (merger_control, antitrust_decision, opinion, interim_measures) |
| `companies_involved` | List of companies involved |
| `sector` | Economic sector(s) |
| `url` | Link to original on autoritedelaconcurrence.fr |

## Notes

- The French version of the dataset is authoritative; only a portion of decisions have been translated to English
- The dataset is published and maintained by the Autorité de la Concurrence itself
- Data is refreshed monthly on data.gouv.fr
