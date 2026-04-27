# US/TaxCourt — United States Tax Court Published Opinions

## Overview
Fetches published opinions from the United States Tax Court via the DAWSON
public API. Covers T.C. Opinions, Memorandum Opinions, and Summary Opinions
from May 1986 to present.

## Data Source
- **API**: DAWSON public API (green/blue endpoints)
- **Search**: `public-api-green.dawson.ustaxcourt.gov/public-api/opinion-search`
- **PDF Download**: Signed S3 URLs via `/public-document-download-url`
- **Rate Limit**: 15 requests per 60-second window

## Opinion Types
| Code | Type | Description |
|------|------|-------------|
| TCOP | T.C. Opinion | Full published Tax Court opinions (precedential) |
| MOP | Memorandum Opinion | Fact-specific decisions applying existing law |
| SOP | Summary Opinion | Small tax case decisions (non-precedential) |

## Usage
```bash
python bootstrap.py test               # Connectivity test
python bootstrap.py bootstrap --sample # Fetch ~12 sample records
python bootstrap.py bootstrap          # Full historical pull
python bootstrap.py update             # Recent 90 days
```

## Authentication
None required. The DAWSON public API is open.

## License

[Public domain](https://www.law.cornell.edu/uscode/text/17/105) — US government works under 17 U.S.C. § 105.

## Notes
- PDF text extraction via `common/pdf_extract`
- Backscrapes in 30-day intervals from 1986 to present
- Green endpoint preferred; blue used as failover
