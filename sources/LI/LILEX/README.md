# LI/LILEX - Liechtenstein Consolidated Legislation

## Overview

LILEX (gesetze.li) is Liechtenstein's official legislation database, operated by the Government Legal Services (Rechtsdienst der Regierung).

## Data Source

- **Website**: https://www.gesetze.li
- **Coverage**: All Liechtenstein legislation from 1921 onwards
- **Language**: German
- **License**: Public domain (official government publication)

## Data Structure

The database provides two views:
1. **Consolidated Law** (`/konso/`): Current versions of all legislation
2. **Legal Gazette** (`/chrono/`): Chronological publication history (LGBl)

### Identifiers

- **LGBl-Nr**: Landesgesetzblatt number (e.g., `1921.015` for the Constitution)
- **LR-Nr**: Systematic register number (e.g., `101`)
- **lgbl_id**: Internal ID format (e.g., `1921015000`)

### URL Patterns

- Overview page: `https://www.gesetze.li/konso/{lgbl_id}`
- Friendly URL: `https://www.gesetze.li/konso/{year}.{number}`
- Full text HTML: `https://www.gesetze.li/konso/html/{lgbl_id}?version={version}`
- PDF download: `https://www.gesetze.li/konso/pdf/{lgbl_id}?version={version}`

## License

Open government data, public domain — official government publications.

## Technical Notes

- No official API available
- Full text is served via iframe from `/konso/html/` endpoints
- Historical versions available with version parameter
- Uses Citrix NetScaler cookie protection (non-blocking)

## Usage

```bash
# Fetch sample documents
python bootstrap.py bootstrap --sample

# List recent laws
python bootstrap.py list --limit 20

# Fetch updates from last 7 days
python bootstrap.py update --days 7
```

## Sample Records

After bootstrap, sample records are saved to the `sample/` directory.

## Key Legislation

- **1921.015** (LR-Nr 101): Constitution of Liechtenstein
- **1926.004**: Civil Code (ABGB)
- **1926.003**: Commercial Code
