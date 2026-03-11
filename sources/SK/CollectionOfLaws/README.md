# SK/CollectionOfLaws — Slovak Collection of Laws

**Source:** Slov-Lex (static.slov-lex.sk)
**Country:** Slovakia (SK)
**Data Type:** Legislation
**Status:** Complete

## Overview

This source fetches Slovak legislation from the official Slov-Lex portal, operated by the Slovak Ministry of Justice. It uses the static HTML version which doesn't require JavaScript.

## Coverage

- **Temporal:** 1918 to present (comprehensive)
- **Document Types:**
  - Zákon (Laws/Acts)
  - Nariadenie vlády (Government Regulations)
  - Vyhláška (Decrees)
  - Rozhodnutie (Decisions)
  - Oznámenie (Announcements/Notices)
  - Nález (Constitutional Court Findings)

## Data Access

Uses the static HTML portal at `static.slov-lex.sk`:

1. **Year Listing:** `/static/SK/ZZ/` - chronological index by year
2. **Laws per Year:** `/static/SK/ZZ/{year}/` - table of all laws for that year
3. **Law Versions:** `/static/SK/ZZ/{year}/{number}/` - version history
4. **Full Text:** `/static/SK/ZZ/{year}/{number}/{date}.html` - complete law text

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (10-15 documents)
python bootstrap.py bootstrap --sample

# Full bootstrap (all legislation)
python bootstrap.py bootstrap

# Incremental update (recent years)
python bootstrap.py update
```

## Sample Output

```json
{
  "_id": "SK/ZZ/2024/7/20240201",
  "_source": "SK/CollectionOfLaws",
  "_type": "legislation",
  "title": "Zákon, ktorým sa mení a dopĺňa zákon č. 575/2001 Z. z. o organizácii činnosti vlády...",
  "text": "ZÁKON zo 16. januára 2024, ktorým sa mení a dopĺňa zákon... [full text]",
  "date": "2024-02-01",
  "url": "https://static.slov-lex.sk/static/SK/ZZ/2024/7/20240201.html",
  "doc_type": "Zákon",
  "year": 2024,
  "number": 7
}
```

## Notes

- Slovakia has ELI (European Legislation Identifier) Pillar I implementation
- The main slov-lex.sk requires JavaScript; we use the static version
- PDF versions are also available but we extract text from HTML for quality
- Rate limited to 1 request per second to respect server load

## License

Open Government Data (similar to CC0) per Slovak public sector data policy.
