# IE/DPC - Irish Data Protection Commission

Data source for Irish Data Protection Commission (DPC) enforcement decisions.

## Overview

Ireland hosts the EU headquarters for most major tech companies (Meta, Google, Apple, Microsoft, TikTok), making the DPC the lead GDPR supervisor for these companies. DPC decisions have pan-European impact and are highly significant for tech/privacy law.

## Data Types

- **case_law**: Enforcement decisions under the Data Protection Act 2018
- **doctrine**: Court judgments involving the DPC

## Access Method

- Scrapes decisions listing page to find decision URLs
- Downloads PDF decisions from individual pages
- Extracts full text using pypdf

## Usage

```bash
# Fetch sample records (15 by default)
python bootstrap.py bootstrap --sample

# Fetch all decisions
python bootstrap.py bootstrap --full
```

## Source URL

https://www.dataprotection.ie/en/dpc-guidance/law/decisions-made-under-data-protection-act-2018

## License

[Irish Public Sector Open Licence](https://data.gov.ie/pages/licence)
