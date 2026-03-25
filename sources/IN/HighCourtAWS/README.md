# IN/HighCourtAWS — Indian High Court Judgments (AWS Open Data)

## Overview

Fetches Indian High Court judgments from the AWS Open Data Registry.
The dataset contains ~16.7 million judgments from 25 High Courts across India,
dating back to 1950.

## Data Source

- **Bucket**: `s3://indian-high-court-judgments` (ap-south-1)
- **Registry**: https://registry.opendata.aws/indian-high-court-judgments/
- **License**: CC-BY-4.0
- **Updates**: Quarterly

## Strategy

1. Lists JSON metadata files via S3 REST API (no AWS credentials needed)
2. Downloads corresponding PDFs from the `data/pdf/` prefix
3. Extracts full text from PDFs using pdfplumber
4. Parses case metadata (parties, judges, dates, CNR) from JSON raw_html field

## S3 Structure

```
metadata/json/year=YYYY/court=X_Y/bench=ZZZ/CNRXXX_N_YYYY-MM-DD.json
data/pdf/year=YYYY/court=X_Y/bench=ZZZ/CNRXXX_N_YYYY-MM-DD.pdf
```

## Usage

```bash
python bootstrap.py test               # Connectivity test
python bootstrap.py bootstrap --sample # Fetch 15 sample records
python bootstrap.py bootstrap          # Full fetch (very large dataset)
python bootstrap.py update             # Incremental update
```

## Notes

- S3 bucket is in ap-south-1 (Mumbai); connections may be slow from other regions
- PDF text extraction works well for English-language judgments
- Some older judgments may be scanned images (no extractable text)
- The full dataset is ~1.11 TB; full bootstrap is not recommended locally
