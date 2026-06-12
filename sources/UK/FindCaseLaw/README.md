# UK/FindCaseLaw — UK Find Case Law (The National Archives)

**Source**: The National Archives
**URL**: https://caselaw.nationalarchives.gov.uk
**Data types**: Case law
**Auth**: None
**License**: Open Justice Licence

## Overview

Official database of ~365,000 court and tribunal judgments for England & Wales,
plus UK-wide Supreme Court and Privy Council cases. Covers UKSC, UKPC, EWCA,
EWHC, and tribunals. Full text is available in Akoma Ntoso XML.

Devolved Scottish court judgments are not covered here; use
`UK/ScottishCourts` for Court of Session, High Court of Justiciary, Sheriff
Appeal Court, Sheriff Court, and Scottish tribunal judgments.

## Data Access

**Atom Feed**: `https://caselaw.nationalarchives.gov.uk/atom.xml`
- Pagination, search, court/tribunal filtering
- 50 results per page, sorted by date

**Full Text XML**: `https://caselaw.nationalarchives.gov.uk/{uri}/data.xml`
- Akoma Ntoso (LegalDocML) format
- Rate limit: 1,000 requests per 5-minute window

## License

[Open Justice Licence](https://caselaw.nationalarchives.gov.uk/open-justice-licence) — open access to court judgments.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # Full pull (~365K docs, ~30 hours)
python bootstrap.py test-api             # Connectivity test
```
