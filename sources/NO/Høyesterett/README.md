# NO/Høyesterett - Norwegian Supreme Court Case Law

Fetches Norwegian court decisions from Lovdata's free public database.

## Data Source

- **Provider**: Lovdata (Norwegian Foundation for Legal Information)
- **URL**: https://lovdata.no/register/avgjørelser
- **RSS Feed**: https://lovdata.no/feed?data=newJudgements&type=RSS
- **License**: Public domain (court decisions are public records under Norwegian law)

## Coverage

- **Supreme Court (Høyesterett)**: Full reasoned judgments from 2008
- **Courts of Appeal (Lagmannsretter)**: Selected decisions from 2008
- **District Courts (Tingretter)**: Selected decisions from 2016

## Court Codes

Case IDs follow the format: `{COURT}-{YEAR}-{NUMBER}-{TYPE}`

| Prefix | Court |
|--------|-------|
| HR | Høyesterett (Supreme Court) |
| LB | Borgarting lagmannsrett |
| LG | Gulating lagmannsrett |
| LH | Hålogaland lagmannsrett |
| LA | Agder lagmannsrett |
| LE | Eidsivating lagmannsrett |
| LF | Frostating lagmannsrett |
| T* | Tingrett (District Court) |

Type suffixes:
- `A`: Avdeling (panel)
- `S`: Storkammer (grand chamber)
- `P`: Plenum
- `U`: Ankeutvalget (appeals committee)

## Usage

```bash
# Fetch sample records
python bootstrap.py bootstrap --sample

# List recent RSS items
python bootstrap.py rss

# Fetch all available decisions
python bootstrap.py fetch
```

## Data Schema

Each record contains:

| Field | Description |
|-------|-------------|
| `_id` | Case ID (e.g., HR-2026-346-U) |
| `_source` | Source identifier (NO/Høyesterett) |
| `_type` | Document type (case_law) |
| `title` | Case ID with keywords |
| `text` | Full text of the decision |
| `date` | Decision date (YYYY-MM-DD) |
| `url` | Link to Lovdata page |
| `court` | Court name |
| `keywords` | Legal keywords/topics |
| `summary` | Case summary |
| `judges` | Names of judges |
| `parties` | Anonymized party names |

## License

Public domain — court decisions are public records under the Norwegian Copyright Act.

## Notes

- Some decisions are marked "Full tekst til avgjørelsen er ikke tilgjengelig" (full text not available) - these are skipped
- Decisions are anonymized: party names replaced with A, B, C, etc.
- Text is in Norwegian (Bokmål)
- Rate limiting: 2 second delay between requests
