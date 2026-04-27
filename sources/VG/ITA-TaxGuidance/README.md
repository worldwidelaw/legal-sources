# VG/ITA-TaxGuidance — BVI International Tax Authority Guidance

## Overview
BVI International Tax Authority guidance documents on CRS, FATCA, CbCR,
Economic Substance, beneficial ownership, TIEAs, and related legislation.

## Data Source
- **Website**: https://bviita.vg (WordPress)
- **API**: WordPress REST API at `/wp-json/wp/v2/`
- **Documents**: ~174 PDFs + 70 posts + 32 pages
- **Auth**: None required

## Content Types
- Guidance notes (CRS, FATCA, CbCR, Economic Substance)
- Legislation (ITA Act, MLAT Act, ES Act, BOSS Act)
- Tax Information Exchange Agreements (27 countries)
- News circulars and announcements
- Forms and user guides

## Usage
```bash
python bootstrap.py test               # Connectivity test
python bootstrap.py bootstrap --sample # Fetch ~12 sample records
python bootstrap.py bootstrap          # Full pull
python bootstrap.py update             # Recent 90 days
```

## License

[Open Government Data](https://bviita.vg) — official guidance and legislation published by the BVI International Tax Authority.
