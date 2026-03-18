# BE/CourConstitutionnelle — Belgian Constitutional Court

**Source:** https://www.const-court.be  
**Data type:** Case law (constitutional review)  
**Coverage:** 1985–present  
**License:** Open Government Data  

## Overview

The Belgian Constitutional Court (Cour constitutionnelle / Grondwettelijk Hof) reviews the constitutionality of laws, decrees, and ordonnances adopted by the federal and federated parliaments.

The court was created in 1980 as the Court of Arbitration (Cour d'Arbitrage) and renamed to Constitutional Court in 2007.

## Data Access

Decisions are published as PDFs with predictable URL patterns:
- **French:** `https://fr.const-court.be/public/f/{YEAR}/{YEAR}-{NNN}f.pdf`
- **Dutch:** `https://nl.const-court.be/public/n/{YEAR}/{YEAR}-{NNN}n.pdf`

No authentication required. Direct HTTP access.

## Sample Statistics

- **100 sample records** fetched
- **Average text length:** 65,463 chars/doc
- **Largest decision:** 357,895 chars (2025/106)
- **Languages:** French, Dutch (separate PDFs)

## ECLI Format

`ECLI:BE:GHCC:{YEAR}:{NUMBER}`

Where:
- `GHCC` = Grondwettelijk Hof / Cour Constitutionnelle
- `YEAR` = Decision year (4 digits)
- `NUMBER` = Sequential decision number

## Usage

```bash
# Test access
python3 bootstrap.py test

# Fetch 100 sample records from recent years
python3 bootstrap.py bootstrap --limit 100

# Fetch all decisions from specific years
python3 bootstrap.py bootstrap --start-year 2024 --end-year 2020 --limit 1000

# FULL HISTORICAL BACKFILL (1985-present, ~5000+ decisions)
# Note: This will take several hours due to rate limiting
python3 bootstrap.py bootstrap --start-year 2026 --end-year 1985
```

## Historical Backfill

The court has published decisions since 1985. A full backfill captures approximately:
- **1985-1990:** ~50 decisions/year (early years)
- **1990-2000:** ~100 decisions/year
- **2000-present:** ~160 decisions/year
- **Total estimate:** ~5,000+ decisions

To run a complete historical backfill:
```bash
python3 bootstrap.py bootstrap --start-year 2026 --end-year 1985
```

## Notes

- ~100-180 decisions published per year
- Full text extracted from PDF using pdfplumber
- Decisions available in both French and Dutch (separate documents)
- Some decisions can exceed 300,000 characters (major constitutional reviews)
