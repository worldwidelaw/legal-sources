# UK/FindCaseLaw — UK Find Case Law (The National Archives)

**Source**: The National Archives
**URL**: https://caselaw.nationalarchives.gov.uk
**Data types**: Case law
**Auth**: None
**License**: Open Justice Licence

## Overview

Official database of ~365,000 court and tribunal judgments for England & Wales.
Covers UKSC, EWCA, EWHC, and tribunals. Full text in Akoma Ntoso XML.

## Data Access

**Atom Feed**: `https://caselaw.nationalarchives.gov.uk/atom.xml`
- Pagination, search, court/tribunal filtering
- 50 results per page, sorted by date

**Full Text XML**: `https://caselaw.nationalarchives.gov.uk/{uri}/data.xml`
- Akoma Ntoso (LegalDocML) format
- Rate limit: 1,000 requests per 5-minute window

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # Full pull (~365K docs, ~30 hours)
python bootstrap.py test-api             # Connectivity test
```
