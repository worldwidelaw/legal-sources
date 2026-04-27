# DE/BSG - German Federal Social Court (Bundessozialgericht)

## Overview

This source fetches case law from the German Federal Social Court (Bundessozialgericht - BSG) via the official rechtsprechung-im-internet.de portal.

## Data Source

- **Portal**: https://www.rechtsprechung-im-internet.de
- **RSS Feed**: https://www.rechtsprechung-im-internet.de/jportal/docs/feed/bsjrs-bsg.xml
- **TOC XML**: https://www.rechtsprechung-im-internet.de/rii-toc.xml
- **Coverage**: ~6,270 decisions from 2010 onwards
- **Format**: XML via ZIP downloads

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).

## Data Structure

Each decision XML contains:
- `doknr` - Document number (unique ID)
- `ecli` - European Case Law Identifier
- `gertyp` - Court type (BSG)
- `spruchkoerper` - Chamber (e.g., "8. Senat")
- `entsch-datum` - Decision date (YYYYMMDD)
- `aktenzeichen` - Case number (e.g., "B 8 SO 6/24 R")
- `doktyp` - Document type (Urteil, Beschluss)
- `tenor` - Operative part of the decision
- `tatbestand` - Facts of the case
- `entscheidungsgruende` - Judicial reasoning

## Usage

Test the fetcher:
```bash
python3 bootstrap.py
```

Create sample dataset (12 records):
```bash
python3 bootstrap.py bootstrap --sample
```

## Technical Notes

- The fetcher downloads ZIP files containing single XML documents
- Each ZIP file is ~30-50 KB
- XML format is documented by DTD at: https://www.rechtsprechung-im-internet.de/dtd/v1/rii-dok.dtd
- Rate limiting: 1.5 seconds between requests
- No authentication required
