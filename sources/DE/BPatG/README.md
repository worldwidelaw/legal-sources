# DE/BPatG - German Federal Patent Court

## Overview

This source fetches case law from the German Federal Patent Court (Bundespatentgericht, BPatG).

## Data Source

- **URL**: https://www.rechtsprechung-im-internet.de
- **Provider**: German Federal Ministry of Justice
- **Coverage**: Decisions from 2010 onwards (~7,000+ decisions)
- **Format**: XML (distributed as ZIP files)
- **Update Frequency**: Daily

## Access Method

The data is accessed through the table of contents XML file at:
`https://www.rechtsprechung-im-internet.de/rii-toc.xml`

Each decision is downloaded as a ZIP file containing XML with full text.

## Data Types

- **Case Law**: Patent appeals, trademark disputes, utility model proceedings

## License

Public domain under German law — [§ 5 UrhG](https://www.gesetze-im-internet.de/urhg/__5.html) (official works / amtliche Werke).

## Fields Captured

- Document ID (doknr)
- ECLI
- Court and chamber
- Decision date
- Case number (Aktenzeichen)
- Decision type
- Relevant norms
- Full text (title, headnote, tenor, reasoning)

## Usage

```bash
# Test with 3 documents
python3 bootstrap.py

# Bootstrap with 12 sample documents
python3 bootstrap.py bootstrap --sample

# Full bootstrap (all documents)
python3 bootstrap.py bootstrap
```
