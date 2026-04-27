# DZ/JORADP - Algerian Official Journal

Journal Officiel de la Republique Algerienne Democratique et Populaire (JORADP)

## Overview

The Official Journal of Algeria publishes all legislation, decrees, orders, and official announcements. It is the authoritative source for Algerian law.

## Data Source

- **Official URL**: https://www.joradp.dz
- **Data Type**: Legislation (official journal issues)
- **Languages**: French (primary), Arabic
- **Coverage**: 1994 - present (PDF format)
- **Update Frequency**: ~80 issues per year (weekly + special editions)

## Access Method

PDFs are freely accessible at predictable URLs:
- French edition: `/FTP/jo-francais/{year}/F{year}{issue:03d}.pdf`
- Arabic edition: `/FTP/jo-arabe/{year}/A{year}{issue:03d}.pdf`

Example: `https://www.joradp.dz/FTP/jo-francais/2026/F2026001.pdf`

## Content

Each issue contains:
- Laws (Lois)
- Decrees (Decrets)
- Executive Decrees (Decrets executifs)
- Orders (Arretes)
- Decisions
- Official announcements

## Technical Details

- PDFs are text-based (not scanned), allowing text extraction
- File sizes range from 100KB to 500KB per issue
- No authentication required
- Rate limiting: 1 request/second recommended

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample data (10+ records)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Update with recent issues
python bootstrap.py update
```

## Dependencies

- PyPDF2 (for PDF text extraction)

## Notes

- The scraper fetches the French edition by default (more accessible for international users)
- Arabic edition is available as fallback
- Pre-1994 data exists in a different format (page-by-page PDFs) but is not currently supported
- Each issue is treated as a single document containing all legal acts published that day

## License

[Open Government Data](https://www.joradp.dz) — official gazette of the People's Democratic Republic of Algeria.
