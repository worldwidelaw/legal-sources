# KG/SupremeCourt — Kyrgyzstan Supreme Court Decisions

**Source:** [Digital Justice Portal (GRSA)](https://portal.sot.kg)
**API:** `https://portal.sot.kg/api/v1/cc_court_case/`
**Auth:** None required
**Language:** Kyrgyz / Russian
**Records:** ~5,000+ Supreme Court decisions (209,000+ across all courts)

## Data Access

The portal.sot.kg provides a fully open REST API (Next.js backend) with no authentication.

Key endpoints:
- `/api/v1/cc_court_case/?page=1&per_page=100&court_id=87&case_act_exist=true` — Supreme Court cases with judicial acts
- `/api/v1/court/` — List of all 73 courts
- `/api/v1/production_type/` — Case type categories

Full text is available as inline HTML in the `file_html` field of each `case_act` entry.

## Usage

```bash
# Fetch sample records
python3 bootstrap.py bootstrap --sample

# Full bootstrap (all Supreme Court decisions)
python3 bootstrap.py bootstrap

# Fetch updates since a date
python3 bootstrap.py updates --since 2025-01-01
```
