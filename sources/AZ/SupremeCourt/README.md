# AZ/SupremeCourt - Azerbaijan Supreme Court

**Source:** https://sc.supremecourt.gov.az/decision-search/
**Country:** Azerbaijan (AZ)
**Data Type:** Case Law
**Language:** Azerbaijani

## Overview

The Azerbaijan Supreme Court (Ali Məhkəməsi) publishes cassation decisions through
a Vue.js search interface backed by a JSON API. This scraper accesses the API directly.

## Coverage

- **~39,000+ decisions** available
- Covers all Supreme Court cassation panels:
  - Criminal (Cinayət)
  - Civil (Mülki)
  - Commercial (Kommersiya)
  - Administrative (İnzibati)
- Decisions from recent years onwards
- Full text extracted from PDF-to-HTML conversion

## API Endpoints

- `POST /decision-search/` - Paginated search (page, perpage params)
- `GET /decision-search/show/{work_no}` - Individual decision details with full text

## Data Fields

| Field | Description |
|-------|-------------|
| work_no | Case number (e.g., "2[1-8]-495/2026") |
| am_date4 | Decision date (DD.MM.YYYY) |
| am_decision_type | Type: Qərar (Decision), Qərardad (Ruling) |
| am_col | Category: 1=Criminal, 2=Civil, 3=Commercial, 4=Administrative |
| am_judge | Presiding judge name |
| am_result | Case outcome/result |
| am_star | Importance rating (0-4 stars) |

## Usage

```bash
# Fetch sample data
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Update with recent decisions
python bootstrap.py update
```

## Notes

- Rate limited to 1 request per 2 seconds
- Full text is HTML extracted from PDF documents
- Star ratings indicate legal significance (4 stars = precedent-setting)
