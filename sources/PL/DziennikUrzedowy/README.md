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
| `/eli/acts/DU/{year}/{pos}/text.html` | Get full text HTML (only where `textHTML: true`) |
| `/eli/acts/DU/{year}/{pos}/text.pdf` | Get full text PDF (available for every act) |

## Full text

Acts carry an HTML rendering only for part of the corpus. 2015-2024 is fully
HTML; **2025 onwards is 100% PDF-only** (`textHTML: false` on every act), and the
pre-2015 archive is mostly PDF too (2005: 309 HTML of 2,260 acts; 1980: 12 of
132). The scraper reads `/text.html` when the listing advertises it and falls
back to `/text.pdf` otherwise, so the PDF path is what reaches most of the
corpus. Acts whose text cannot be extracted in either format are skipped rather
than emitted with an empty `text` field.

Years are crawled newest-first and each completed year is recorded in
`data/years_done.json`, so a re-launch resumes at the first unfinished year
instead of re-crawling ~100K acts from the top.

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

# Full bootstrap (bootstrap-fast is an alias)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

[Open Government Data](https://dane.gov.pl) — free for reuse under Polish open data policy.
