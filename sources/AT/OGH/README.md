# AT/OGH - Austrian Supreme Court (Oberster Gerichtshof)

## Overview

Fetches case law from the Austrian Supreme Court (Oberster Gerichtshof - OGH) via the RIS Open Government Data API v2.6.

## Data Source

- **URL**: https://www.ris.bka.gv.at/Jus/
- **API**: https://data.bka.gv.at/ris/api/v2.6/Judikatur
- **Application**: Justiz (with Gericht=OGH filter)
- **Records**: 131,000+ decisions
- **License**: CC BY 4.0 (Austrian Open Government Data)

## Coverage

- Civil and criminal supreme court decisions
- Both Rechtssatz (legal principles) and Entscheidungstext (decision texts)
- ECLI identifiers (ECLI:AT:OGH0002:YYYY:...)
- Full text available via XML/HTML content URLs

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (131K+ records - takes many hours)
python bootstrap.py bootstrap

# Incremental updates
python bootstrap.py update
```

## Schema

Key fields in normalized records:
- `_id`: Document ID
- `ecli`: European Case Law Identifier
- `geschaeftszahl`: Case number
- `gericht`: Court (OGH)
- `entscheidungsdatum`: Decision date
- `rechtsgebiete`: Legal areas (Zivilrecht, Strafrecht, etc.)
- `normen`: Referenced legal norms
- `text`: Full decision text (MANDATORY)

## Related Sources

- AT/RIS: Federal legislation and case law from all Austrian courts
- AT/VfGH: Austrian Constitutional Court
- AT/Bundesgesetzblatt: Federal Law Gazette

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Austrian Open Government Data.
