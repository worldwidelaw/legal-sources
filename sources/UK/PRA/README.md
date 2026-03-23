# UK/PRA - Prudential Regulation Authority

## Overview
Fetches PRA publications from the Bank of England website, including policy statements,
supervisory statements, consultation papers, and letters covering banking and insurance
prudential regulation.

## Data Source
- **URL**: https://www.bankofengland.co.uk/prudential-regulation
- **Method**: BoE internal News API (discovery) + HTML extraction (full text)
- **Auth**: None required
- **License**: Open Government Licence v3.0
- **Documents**: ~1,275 publications (2012-present)

## API Details
- **Discovery endpoint**: `POST /_api/News/RefreshPagedNewsList`
- **Data source ID**: `CE377CC8-BFBC-418B-B4D9-DBC1C64774A8`
- **Pagination**: 30 items per page, ~43 pages total
- **Full text**: Extracted from individual publication HTML pages

## Publication Types
- Policy statements (PS)
- Supervisory statements (SS)
- Consultation papers (CP)
- Letters
- Discussion papers
- Regulatory digests

## Usage
```bash
python bootstrap.py test                  # Test connectivity
python bootstrap.py bootstrap --sample    # Fetch 15 sample records
python bootstrap.py bootstrap             # Full fetch
```
