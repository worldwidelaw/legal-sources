# AT/Bundesgesetzblatt - Austrian Federal Law Gazette

## Overview

The Bundesgesetzblatt (BGBl) is Austria's official journal for publishing federal legislation. Since January 1, 2004, the electronic version published on RIS is legally binding ("authentisch").

## Data Source

- **API**: RIS OGD API v2.6
- **Endpoint**: `https://data.bka.gv.at/ris/api/v2.6/Bundesrecht`
- **Application**: `BgblAuth`
- **Authentication**: None (Open Government Data)
- **License**: CC BY 4.0

## Coverage

The authentic electronic BGBl covers three parts:

| Part | German Name | Content | Records |
|------|-------------|---------|---------|
| BGBl I | Teil1 | Federal laws (Bundesgesetze) | ~3K |
| BGBl II | Teil2 | Regulations (Verordnungen) | ~12K |
| BGBl III | Teil3 | International law, treaties | ~3K |

**Total**: ~18,000+ authentic gazette entries (2004-present)

## Document Types

- **Bundesgesetz**: Federal law passed by Parliament
- **Verordnung**: Regulation/ordinance issued by ministry
- **Kundmachung**: Official announcement/proclamation
- **Staatsvertrag**: International treaty
- **Wiederverlautbarung**: Republication of amended law

## ELI URIs

Austrian BGBl uses European Legislation Identifier (ELI) URIs:

```
https://www.ris.bka.gv.at/eli/bgbl/{I,II,III}/{year}/{number}/{date}
```

Example: `https://www.ris.bka.gv.at/eli/bgbl/I/2026/1/20260115`

## Full Text Access

Each document provides content URLs in multiple formats:
- XML (structured, preferred for text extraction)
- HTML (web display)
- RTF (word processor)
- PDF (authentic signed version)

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (10+)
python bootstrap.py bootstrap --sample

# Full bootstrap (~18K records)
python bootstrap.py bootstrap

# Incremental update (last month)
python bootstrap.py update
```

## Difference from AT/RIS

- **AT/RIS (BrKons)**: Consolidated federal law - shows current state of laws with all amendments incorporated
- **AT/Bundesgesetzblatt (BgblAuth)**: Authentic gazette - original text as published, immutable record of what was enacted

Use BGBl for legal citation and historical research. Use consolidated law for current legal state.
