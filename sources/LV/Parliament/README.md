# LV/Parliament — Latvian Parliament (Saeima) Transcripts

## Overview

This source fetches parliamentary debate transcripts from the Latvian Saeima (Parliament) website.

**Source URL:** https://www.saeima.lv/lv/transcripts/
**Data Type:** Parliamentary proceedings (stenogrammas)
**Language:** Latvian
**License:** Public domain (government documents)

## Coverage

- **5th Saeima** (1993-1995): ~144 transcripts
- **6th Saeima** (1995-1998): ~36 transcripts
- **7th Saeima** (1998-2002): ~209 transcripts
- **8th Saeima** (2002-2006): ~211 transcripts
- **11th Saeima** (2011-2014): ~156 transcripts
- **12th Saeima** (2014-2018): ~237 transcripts
- **13th Saeima** (2018-2022): ~439 transcripts
- **14th Saeima** (2022-present): ~199 transcripts (ongoing)

**Total:** ~1,600+ transcripts

## Data Access Strategy

1. **Discovery:** Category pages at `/lv/transcripts/category/{ID}` list transcript links
2. **Fetch:** Individual transcripts at `/lv/transcripts/view/{ID}` contain full stenogram HTML
3. **Parse:** Extract text from `<p>` tags, clean HTML entities

## Output Schema

| Field | Type | Description |
|-------|------|-------------|
| `_id` | string | Transcript ID |
| `_source` | string | "LV/Parliament" |
| `_type` | string | "parliamentary_proceedings" |
| `title` | string | Session title (e.g., "15/22") |
| `text` | string | Full stenogram text |
| `date` | string | Session date (YYYY-MM-DD) |
| `url` | string | Link to original transcript |
| `saeima_number` | int | Saeima term number (5-14) |
| `session_leader` | string | Session chair name |
| `language` | string | "lv" |

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all transcripts)
python bootstrap.py bootstrap

# Incremental update (recent transcripts only)
python bootstrap.py update
```

## Notes

- Transcripts contain full debate text with speaker attributions
- Average transcript size: ~200-300KB of text
- Rate limited to 1 request/second to be respectful
- Older transcripts (5th-8th Saeima) may have encoding issues with Latvian characters
