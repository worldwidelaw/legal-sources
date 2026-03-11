# PL/DziennikUrzedowy - Polish Official Journal

## Overview

Dziennik Ustaw (Official Journal of the Republic of Poland) is the primary source for Polish legislation. This scraper uses the ELI API provided by the Sejm (Polish Parliament) to fetch acts with full text.

## Data Source

- **Website**: https://www.dziennikustaw.gov.pl
- **API**: https://api.sejm.gov.pl/eli
- **Coverage**: 1918 to present
- **Volume**: ~2,000 acts per year

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/eli/acts/DU/{year}` | List all acts for a year |
| `/eli/acts/DU/{year}/{pos}` | Get act metadata |
| `/eli/acts/DU/{year}/{pos}/text.html` | Get full text HTML |

## Document Types

- Ustawa (Law)
- Rozporządzenie (Regulation)
- Obwieszczenie (Announcement)
- Umowa międzynarodowa (International treaty)

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

Open Government Data - free for reuse.
