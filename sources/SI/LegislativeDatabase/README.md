# SI/LegislativeDatabase - Slovenian Legislation Database (PISRS)

## Overview

This source fetches Slovenian laws (zakoni) adopted by the National Assembly
(Državni zbor) since independence on June 25, 1991.

## Data Sources

### Metadata
- **Source**: Parliament Open Data (fotogalerija.dz-rs.si)
- **Format**: XML file containing all adopted laws
- **URL**: https://fotogalerija.dz-rs.si/datoteke/opendata/SZ.XML
- **License**: CC BY 4.0

### Full Text
- **Source**: Official Gazette of the Republic of Slovenia (Uradni list RS)
- **Format**: HTML with structured segments (articles, sections)
- **URL pattern**: https://www.uradni-list.si/glasilo-uradni-list-rs/vsebina/{sop_number}

## Key Fields

| Field | Description |
|-------|-------------|
| `sop_number` | Official Publication Number (e.g., 1997-01-1842) |
| `title` | Law title in Slovenian |
| `abbreviation` | Law abbreviation (e.g., ZOsn-A, ZPIZ-2) |
| `date` | Date of adoption (ISO 8601) |
| `text` | Full text of the law |
| `publication` | Official Gazette reference |
| `keywords` | Subject keywords |

## Usage

```bash
# Fetch sample records (10-15)
python bootstrap.py bootstrap --sample

# Fetch all records (thousands of laws)
python bootstrap.py bootstrap --full
```

## Notes

- The Parliament XML contains metadata only; full text must be fetched from
  the Official Gazette website.
- Rate limiting is applied (2 seconds between requests) to respect the
  Official Gazette server.
- Some older laws may not have full text available online.

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Parliament open data licensed under Creative Commons Attribution 4.0.

## References

- [PISRS Portal](https://www.pisrs.si) - Legal Information System of Slovenia
- [Parliament Open Data](https://podatki.gov.si/dataset/dzsprejeti-zakoni)
- [Official Gazette](https://www.uradni-list.si)
- [EUR-Lex ELI Registry - Slovenia](https://eur-lex.europa.eu/eli-register/slovenia.html)
