# AT/RIS -- Rechtsinformationssystem (Austrian Legal Information System)

**Country:** Austria (AT)
**Source:** [RIS - Bundeskanzleramt](https://www.ris.bka.gv.at)
**API:** [OGD API v2.6](https://data.bka.gv.at/ris/api/v2.6/)
**License:** CC BY 4.0 (Austrian Open Government Data)
**Auth:** None required

## Data Coverage

| Category | Application | Records | Description |
|----------|------------|---------|-------------|
| Federal Law | BrKons | ~436K | Consolidated federal legislation |
| Case Law | Justiz | ~138K | General courts (OGH, OLG, LG, BG) |
| Case Law | Vfgh | varies | Constitutional Court |
| Case Law | Vwgh | varies | Supreme Administrative Court |
| Case Law | Bvwg | varies | Federal Administrative Court |
| Case Law | Lvwg | varies | State Administrative Courts |

## Key Fields

### Legislation (Bundesrecht)
- `doc_id` - RIS norm ID (e.g., NOR40258921)
- `title` - Short title (Kurztitel)
- `eli` - European Legislation Identifier
- `date_effective` - Entry into force date
- `date_expired` - Expiry date (if repealed)
- `gesetzesnummer` - Law number
- `indizes` - Systematic classification

### Case Law (Judikatur)
- `doc_id` - RIS judgment ID (e.g., JJT_20260130_...)
- `geschaeftszahl` - Case number
- `ecli` - European Case Law Identifier
- `entscheidungsdatum` - Decision date
- `gericht` - Court name
- `normen` - Referenced legal norms
- `rechtsgebiete` - Legal areas

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (10 legislation + case law)
python bootstrap.py bootstrap --sample

# Full bootstrap (WARNING: 500K+ records, run off-hours)
python bootstrap.py bootstrap

# Incremental update (last month)
python bootstrap.py update
```

## Rate Limiting

- No formal rate limit documented
- We use 0.5 req/s (1 request every 2 seconds) for safety
- For bulk downloads, notify ris.it@bka.gv.at in advance
- Prefer off-hours (18:00-06:00 CET) for large fetches

## API Notes

- Max 100 results per page (`DokumenteProSeite=OneHundred`)
- `ImRisSeit` filter supports: EinerWoche, ZweiWochen, EinemMonat, DreiMonaten, SechsMonaten, EinemJahr
- Full document text available via content URLs in XML, HTML, RTF, and PDF formats
- Response structure: `OgdSearchResult.OgdDocumentResults.OgdDocumentReference[]`

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Austrian Open Government Data.
