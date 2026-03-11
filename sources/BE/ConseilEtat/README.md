# BE/ConseilEtat — Belgian Council of State

**Source:** https://www.raadvst-consetat.be  
**Data type:** Case law (administrative)  
**Coverage:** 1994–present (digital)  
**License:** Open Government Data  

## Overview

The Belgian Council of State (Conseil d'État / Raad van State) is the supreme administrative court of Belgium. It handles:

- Administrative disputes (urbanisme, fonction publique, marchés publics, etc.)
- Advisory opinions on draft legislation
- Cassation appeals in administrative matters

Note: Decisions concerning aliens (foreigners) law are generally not published to protect vulnerable individuals like refugees.

## Data Access

Decisions are available as PDFs via direct URL:
- `https://www.raadvst-consetat.be/arr.php?nr={NUMBER}&l=fr` (French)
- `https://www.raadvst-consetat.be/arr.php?nr={NUMBER}&l=nl` (Dutch)

Decision numbers are sequential (e.g., 265560 for 2026).

Recent decisions are listed at:
- `https://www.raadvst-consetat.be/?lang=fr&page=lastmonth_{MM}`

No authentication required. Direct HTTP access.

## Sample Statistics

- **100 sample records** fetched
- **Average text length:** 17,903 chars/doc
- **Largest decision:** 82,189 chars (265479)
- **Languages:** French, Dutch (separate documents)

## Decision Number Ranges (approximate)

- 2026: 265,000+
- 2025: 255,000 - 265,000
- 2024: 250,000 - 255,000
- 2023: 245,000 - 250,000
- Historical: 1994 starts around 47,000

## Usage

```bash
# Test access
python3 bootstrap.py test

# Fetch 100 sample records
python3 bootstrap.py bootstrap --limit 100

# Fetch specific range
python3 bootstrap.py bootstrap --start-nr 265000 --end-nr 264000 --limit 500
```

## Notes

- ~5,000+ decisions published per year (excluding aliens cases)
- Full text extracted from PDF using pdfplumber
- Decisions categorized by subject matter (urbanisme, fonction publique, marchés publics, etc.)
- Both arrêts (judgments) and ordonnances (cassation non-admissions) available
