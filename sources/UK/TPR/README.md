# UK/TPR - The Pensions Regulator

## Overview
Fetches TPR publications including codes of practice, regulatory guidance,
enforcement activity reports, and consultation papers covering UK pension
scheme regulation.

## Data Source
- **URL**: https://www.thepensionsregulator.gov.uk
- **Method**: Sitemap parsing + HTML extraction
- **Auth**: None required
- **License**: Open Government Licence v3.0
- **Documents**: ~523 English content pages

## Usage
```bash
python bootstrap.py test                  # Test connectivity
python bootstrap.py bootstrap --sample    # Fetch 15 sample records
python bootstrap.py bootstrap             # Full fetch
```
