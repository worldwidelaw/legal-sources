# UK/FindCaseLaw — UK Find Case Law (The National Archives)

**Source**: The National Archives
**URL**: https://caselaw.nationalarchives.gov.uk
**Data types**: Case law
**Auth**: None
**License**: Open Justice Licence v2.0 for ordinary reuse; separate TNA computational-analysis licence required for LDH-style indexing/search

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

## License

> ⚠️ **Computational analysis restricted.** The Open Justice Licence v2.0 permits
> many ordinary uses of Find Case Law judgments, including legal research,
> citation, education, journalism, and some commercial product use. The National
> Archives requires a separate licence for computational analysis, including bulk
> programmatic searching, text mining/NLP, AI or LLM use, and building
> semantic/vector-search indexes.

[Open Justice Licence v2.0](https://caselaw.nationalarchives.gov.uk/open-justice-licence) — ordinary judgment re-use.

[When you need permission](https://caselaw.nationalarchives.gov.uk/when-you-need-permission) — TNA guidance requiring a separate licence for computational analysis and bulk programmatic searching.

Treat this source as **licence-scope pending for Legal Data Hunter indexing and
retrieval** until a separate TNA computational-analysis licence is confirmed.

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # Full pull (~365K docs, ~30 hours)
python bootstrap.py test-api             # Connectivity test
```
