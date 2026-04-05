# JP/NTA-Circulars — Japanese National Tax Agency Circulars

## Overview
Administrative circulars (通達/tsutatsu) from the Japanese National Tax Agency (NTA).
These are authoritative interpretive guidance documents covering all areas of Japanese
tax law.

## Coverage
- Income Tax (所得税)
- Corporate Tax (法人税)
- Inheritance & Gift Tax (相続税・贈与税)
- Asset Valuation (財産評価)
- Consumption Tax (消費税)
- Stamp Tax (印紙税)
- Various excise taxes (petroleum, tobacco, liquor, etc.)
- Tax collection and enforcement
- Appeals procedures
- Tax attorney regulation

## Data Source
- URL: https://www.nta.go.jp/law/tsutatsu/menu.htm
- Format: HTML pages
- Language: Japanese
- Auth: None (open access)

## Strategy
1. Start from known TOC pages for each tax category
2. Extract links to individual section/circular pages
3. Fetch each page and extract full text from HTML
4. For kobetsu (individual) circulars, two-level crawl needed

## Usage
```bash
python bootstrap.py test               # Connectivity test
python bootstrap.py bootstrap --sample # ~15 sample records
python bootstrap.py bootstrap          # Full crawl
```
