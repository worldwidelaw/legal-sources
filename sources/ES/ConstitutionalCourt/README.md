# ES/ConstitutionalCourt - Spanish Constitutional Court

**Source:** Tribunal Constitucional de España - Sistema HJ
**URL:** https://hj.tribunalconstitucional.es
**Country:** Spain (ES)
**Data Type:** Case Law
**License:** Public Domain (official court decisions — see [License](#license) below)

## Overview

This source fetches Constitutional Court decisions (sentencias, autos, declaraciones) from the Sistema HJ - Buscador de jurisprudencia constitucional. The Constitutional Court of Spain (Tribunal Constitucional) has been operating since 1980 and is the supreme interpreter of the Spanish Constitution.

## Data Access

The HJ System provides direct HTML access to court decisions via sequential IDs:

- **Resolution page:** `https://hj.tribunalconstitucional.es/HJ/es/Resolucion/Show/{id}`
- **Document download:** `https://hj.tribunalconstitucional.es/HJ/es/Resolucion/GetDocumentResolucion/{id}`

IDs range from 1 (SENTENCIA 1/1981 - first decision) to ~32,000+ (current).

## Resolution Types

- **SENTENCIA**: Judgment - full constitutional rulings
- **AUTO**: Order - procedural decisions
- **DECLARACION**: Declaration - constitutional declarations

## Document Structure

Each resolution includes:
- **Antecedentes**: Background/procedural history
- **Fundamentos jurídicos**: Legal reasoning
- **Fallo**: Ruling/decision
- **Votos particulares**: Dissenting/concurring opinions (if any)

## Identifiers

- **ECLI**: European Case Law Identifier (e.g., `ECLI:ES:TC:2024:123`)
- **Resolution number/year**: Traditional citation (e.g., `SENTENCIA 123/2024`)
- **BOE reference**: Official Gazette publication

## Usage

```bash
# Run connectivity test
python bootstrap.py test

# Fetch sample records (12 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (all ~32,000 decisions)
python bootstrap.py bootstrap

# Incremental update (new decisions since last run)
python bootstrap.py update
```

## Sample Data

After running `--sample`, check the `sample/` directory for example records:
- Individual JSON files: `record_0000.json`, `record_0001.json`, etc.
- Combined file: `all_samples.json`

## Rate Limiting

- 1 request per second (conservative to avoid blocking)
- No authentication required
- Public data, freely accessible

## Languages

Decisions are primarily in Spanish. Some have translations:
- Spanish: `/HJ/es/Resolucion/Show/{id}`
- English: `/HJ/en/Resolucion/Show/{id}`
- French: `/HJ/fr/Resolucion/Show/{id}`

## License

Open government data under [Spanish Reuse of Public Sector Information regulations](https://datos.gob.es/en/terms). Official court decisions are public domain.

## Notes

- The HJ System is the official search engine for Constitutional Court jurisprudence
- Full text includes all sections of the decision
- BOE PDF links available for official gazette publication
- Covers all decisions from 1981 to present
