# BG/CPDP - Bulgarian Commission for Personal Data Protection

## Overview

Fetches decisions and opinions from the Bulgarian Commission for Personal Data Protection (Комисия за защита на личните данни / КЗЛД).

**URL:** https://cpdp.bg
**Data types:** regulatory_decisions
**Auth:** none

## Data Source

The CPDP website is built on WordPress and exposes a REST API at `/wp-json/wp/v2/`.

### API Endpoints

- **Categories:** `GET /wp-json/wp/v2/categories` - List all categories
- **Posts:** `GET /wp-json/wp/v2/posts?categories={id}` - Get posts by category

### Coverage

- **Decisions (Решения):** 2007-2023 (~830+ decisions)
- **Opinions (Становища):** 2007-2026 (~400+ opinions)

## Content Structure

Each document contains:
- Full text of the decision/opinion (in Bulgarian)
- Date of publication
- Decision/opinion number
- Direct URL to the document

## Usage

```bash
# Sample mode (10+ records)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Update since last run
python bootstrap.py update
```

## Notes

- Language: Bulgarian (UTF-8)
- SSL certificate has issues - verify is disabled
- Content is in HTML format, cleaned to plain text
- Documents are categorized by year
