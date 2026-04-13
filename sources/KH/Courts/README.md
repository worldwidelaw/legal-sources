# KH/Courts — Extraordinary Chambers in the Courts of Cambodia (ECCC)

## Overview
Fetches decisions, orders, and judgments from the ECCC (Khmer Rouge Tribunal)
archive at `archive.eccc.gov.kh`.

## Data Access
- **API**: REST JSON API at `https://archive.eccc.gov.kh/api/search` (POST)
- **Auth**: None required
- **Documents**: PDF download via `/api/documents/{id}/download?matterId=49`
- **Text extraction**: pdfminer (falls back to empty string if extraction fails)

## Record Types
| Type | Count | Description |
|------|-------|-------------|
| Decision | 4,983 | Procedural and substantive decisions |
| Order | 2,714 | Court orders |
| Judgment | 28 | Final judgments |

## Usage
```bash
python bootstrap.py bootstrap --sample   # Fetch 15 sample records
python bootstrap.py bootstrap            # Full bootstrap (all decisions/orders/judgments)
```
