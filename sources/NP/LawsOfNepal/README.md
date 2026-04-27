# NP/LawsOfNepal — Nepal Law Commission: Laws of Nepal

**Source:** https://lawcommission.gov.np/
**Country:** Nepal (NP)
**Data type:** Legislation
**Language:** Nepali (ne)
**Auth:** None (government open data)

## Overview

Official collection of Nepal's prevailing legislation published by the Nepal
Law Commission. Acts are organized into 17 volumes by subject matter and
available as PDFs on the government CDN.

## Strategy

1. Crawl 17 volume category pages (paginated HTML tables)
2. Extract act metadata: title, date, PDF URL, content ID
3. Download PDFs from `giwmscdntwo.gov.np` CDN
4. Extract full text using PyPDF2

## Data

- ~340 acts covering all areas of Nepali law
- Full text in Nepali (Devanagari script)
- Dates in Bikram Sambat calendar

## Usage

```bash
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py test-api             # Connectivity test
```

## License

[Open Government Data](https://lawcommission.gov.np/) — official legislation published by the Nepal Law Commission.
