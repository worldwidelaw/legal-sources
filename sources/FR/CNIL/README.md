# FR/CNIL - Commission Nationale de l'Informatique et des Libertés

French data protection authority (DPA) deliberations.

## Data Source

- **Provider:** DILA (Direction de l'Information Légale et Administrative)
- **URL:** https://echanges.dila.gouv.fr/OPENDATA/CNIL/
- **Format:** XML (tar.gz archives)
- **License:** Licence Ouverte 2.0 (Open License)
- **Also available on:** Légifrance

## Coverage

All CNIL deliberations since the authority's creation in 1979:
- **Sanctions (SAN):** Enforcement decisions with fines under GDPR/LIL
- **Research authorizations (DR):** Approvals for processing health data
- **Other authorizations:** International transfer permits, derogations
- **Opinions & recommendations:** Advisory decisions on data protection

## Usage

```bash
# Generate sample data (15 records)
python3 bootstrap.py bootstrap --sample --count 15

# Fetch all deliberations (full archive)
python3 bootstrap.py bootstrap

# Fetch recent updates since date
python3 bootstrap.py updates --since 2025-01-01
```

## Schema

| Field | Type | Description |
|-------|------|-------------|
| `_id` | string | CNIL document ID (CNILTEXT...) |
| `title` | string | Full deliberation title |
| `text` | string | Complete decision text (cleaned HTML) |
| `date` | string | Decision date (YYYY-MM-DD) |
| `date_publi` | string | Publication date |
| `numero` | string | Deliberation number (e.g., SAN-2026-001) |
| `nature_delib` | string | Type (Sanction, Autorisation de recherche, etc.) |
| `url` | string | Légifrance URL |

## Statistics

- ~5,000+ deliberations total (1979-present)
- Average text length: ~3,500 characters
- Monthly updates via incremental archives

## License

[Licence Ouverte 2.0 / Open Licence (Etalab)](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) — free reuse with attribution.
