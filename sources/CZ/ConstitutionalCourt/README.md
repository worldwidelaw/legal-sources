# Czech Constitutional Court (Ústavní soud)

Data source for decisions of the Constitutional Court of the Czech Republic.

## Source Information

- **Official Website**: https://www.usoud.cz
- **Database**: NALUS (https://nalus.usoud.cz)
- **Data Type**: Case law (decisions, rulings, opinions)
- **Language**: Czech
- **Coverage**: 1993 - present (~70,000+ decisions)
- **License**: Open access (public government data)

## Access Method

This fetcher uses the NALUS GetText.aspx endpoint which provides full text
access to decisions via case reference numbers (spisová značka).

### Endpoint

```
https://nalus.usoud.cz/Search/GetText.aspx?sz={case_ref}
```

### Case Reference Format

Format: `{senate}-{number}-{year}`

- **Senate**: 1, 2, 3, 4 (corresponds to I, II, III, IV), or Pl (Plenum)
- **Number**: Case number within the year
- **Year**: Last 2 digits of the year

Example: `1-709-05` = I. ÚS 709/05

### ECLI Format

The European Case Law Identifier for Czech Constitutional Court decisions:

```
ECLI:CZ:US:{year}:{senate}.US.{number}.{year}.{ordinal}
```

## Usage

```bash
# Test the fetcher (fetches 3 decisions)
python3 bootstrap.py

# Run full bootstrap (fetches ~12 sample decisions)
python3 bootstrap.py bootstrap --sample
```

## Data Schema

Each normalized record contains:

| Field | Description |
|-------|-------------|
| `_id` | ECLI identifier |
| `_source` | "CZ/ConstitutionalCourt" |
| `_type` | "case_law" |
| `title` | Case description or reference |
| `case_reference` | Human-readable case reference (e.g., "I. ÚS 709/05") |
| `ecli` | ECLI identifier |
| `text` | Full text of the decision |
| `decision_type` | NÁLEZ (judgment), USNESENÍ (resolution), or STANOVISKO (opinion) |
| `date` | Decision date |
| `year` | Year of decision |
| `senate` | Senate number (1-4 or Pl) |
| `url` | Direct link to NALUS page |
| `language` | "cs" |

## Notes

- The NALUS website uses ASP.NET with session-based forms
- Direct GET requests to GetText.aspx work without session cookies
- Rate limiting is set to 1 request per second to be respectful
- Very short responses (<1000 chars) usually indicate errors or missing cases
