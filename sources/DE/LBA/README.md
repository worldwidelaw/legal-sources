# DE/LBA - German Federal Aviation Authority Airworthiness Directives

## Overview

This source fetches Airworthiness Directives (Lufttüchtigkeitsanweisungen - LTAs) from the German Luftfahrt-Bundesamt (LBA), the national civil aviation authority of Germany.

## Data Source

- **Website**: https://www2.lba.de/LTAs/
- **Authority**: Luftfahrt-Bundesamt (Federal Aviation Authority)
- **Type**: Regulatory Decisions
- **Format**: PDF (full text extracted)

## What are Airworthiness Directives?

Airworthiness Directives (LTAs) are legally binding regulatory decisions issued by aviation authorities that mandate specific actions to ensure the continued airworthiness of aircraft. They address safety issues discovered in aircraft, engines, propellers, or other components.

## Data Structure

Each LTA contains:
- **LTA Number**: Unique identifier (format: YYYY-NNN or YYYY-NNNRX for revisions)
- **Publication Date**: When the directive was issued
- **Aircraft Type**: Type of aircraft affected
- **Manufacturer**: Type Certificate holder
- **Model**: Specific aircraft model(s)
- **Subject**: Description of the issue being addressed
- **Full Text**: Complete directive content including requirements and compliance deadlines

## Coverage

- **Period**: 1990 to present
- **Volume**: Approximately 250-300 LTAs per year
- **Total**: ~9,000+ directives in the archive

## Technical Details

### Data Access
- PDFs available at predictable URLs: `https://www2.lba.de/ltadocs/{LTA-Nr}.pdf`
- No authentication required
- Rate limiting: 1 request per second recommended

### Text Extraction
- PDFs are downloaded and text extracted using PyPDF2
- Metadata parsed from standardized document structure

## Usage

```bash
# Test with 3 documents
python3 bootstrap.py

# Bootstrap with 10 sample documents
python3 bootstrap.py bootstrap --sample

# Full bootstrap (may take many hours)
python3 bootstrap.py bootstrap
```

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).
