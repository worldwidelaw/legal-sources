# OM/DecreeOm — Oman Decrees Portal

**Source:** https://decree.om/
**Country:** Oman (OM)
**Data type:** Legislation
**Language:** English (translations of Arabic originals)
**Auth:** None (WordPress REST API, open access)

## Overview

Comprehensive database of English-translated Omani legislation including
Royal Decrees, Ministerial Decisions, Consolidated Laws, Regulations, and Treaties.

## Strategy

Uses WordPress REST API (`/wp-json/wp/v2/posts`) to paginate through all categories.
HTML content cleaned to plain text. ~10,000+ legal instruments.

## Categories

| Category | Count |
|----------|-------|
| Royal Decrees | 4,876 |
| Ministerial Decisions | 4,549 |
| Treaties | 679 |
| Consolidated Laws | 95 |
| Consolidated Regulations | 55 |

## Usage

```bash
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py test-api             # Connectivity test
```
