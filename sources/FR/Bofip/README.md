# FR/Bofip - Bulletin Officiel des Finances Publiques

## Overview

This source fetches French tax administration doctrine from the BOFiP-Impôts
(Bulletin Officiel des Finances Publiques - Impôts) database via the official
open data API.

## Data Source

- **API**: https://data.economie.gouv.fr/explore/dataset/bofip-vigueur/api/
- **Portal**: https://bofip.impots.gouv.fr/
- **Publisher**: DGFIP (Direction Générale des Finances Publiques)
- **License**: Licence Ouverte v2.0 (Etalab)

## Content

BOFiP-Impôts contains:

- Administrative comments on legislative and regulatory provisions of fiscal scope
- Rescrit decisions of general scope
- Innovative ministerial responses
- Comments on court decisions affecting tax doctrine

## Coverage

- **Total records**: ~9,000 active publications
- **Update frequency**: Weekly
- **Series covered**: TVA, IR, IS, CTX, BIC, BNC, PAT, ENR, etc.

## Usage

```bash
# Generate sample data (15 records)
python3 bootstrap.py bootstrap --sample

# Full bootstrap (all records)
python3 bootstrap.py bootstrap

# Fetch updates since a date
python3 bootstrap.py updates --since 2025-01-01
```

## Record Schema

| Field | Description |
|-------|-------------|
| `_id` | BOI identifier (e.g., `BOI-TVA-DECLA-20-30-20-30`) |
| `title` | Full document title |
| `text` | Full text content (HTML cleaned) |
| `date` | Validity start date |
| `serie` | Tax series (TVA, IR, IS, etc.) |
| `division` | Document division within series |
| `type` | Document type (Contenu, Actualité, etc.) |
| `url` | Permalink to official source |
