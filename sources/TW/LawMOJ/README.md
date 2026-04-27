# TW/LawMOJ — Taiwan Laws & Regulations Database (MOJ)

**Source**: Ministry of Justice, Republic of China (Taiwan)
**URL**: https://law.moj.gov.tw
**Data types**: Legislation (laws + orders/regulations)
**Auth**: None (Open Government Data)
**License**: Government Open Data License (Taiwan)

## Overview

Taiwan's official consolidated legislation database containing 11,752+ laws and
administrative orders/regulations. Published by the Ministry of Justice with both
Chinese and English translations (English available for ~10% of laws).

## Data Access

**Primary**: Official MOJ bulk JSON API at `https://law.moj.gov.tw/api/`
- Returns ZIP files containing all laws in JSON format
- Endpoints: `/ch/law/json`, `/ch/order/json`, `/en/law/json`, `/en/order/json`
- Swagger docs: https://law.moj.gov.tw/api/swagger

**Fallback**: GitHub mirror at `kong0107/mojLawSplitJSON`
- Individual JSON files per law, updated monthly
- Used when MOJ servers are unreachable (common from non-Asian locations)

## Record Schema

| Field | Description |
|-------|-------------|
| pcode | Unique law code (e.g., A0000001) |
| title | Chinese law title |
| title_en | English title (if available) |
| text | Full article text (Chinese) |
| text_en | Full article text (English, if available) |
| date | Last amendment date (ISO 8601) |
| url | Official URL on law.moj.gov.tw |
| nature | Law type (憲法, 法律, 命令) |
| category | Legal classification |
| preamble | Preamble text |
| history | Legislative amendment history |

## Usage

```bash
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # Full pull (11,752+ laws)
python bootstrap.py test-api             # Connectivity test
```

## License

[Government Open Data License (Taiwan)](https://data.gov.tw/license) — official legislation published by the Ministry of Justice, Republic of China (Taiwan).
