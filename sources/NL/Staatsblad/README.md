# NL/Staatsblad - Dutch Official Gazette

**Staatsblad van het Koninkrijk der Nederlanden** (State Gazette of the Kingdom of the Netherlands)

## Overview

This source fetches Dutch national legislation from the Staatsblad, the official publication medium for Dutch laws and royal decrees. All legislation must be published in the Staatsblad to enter into force.

## Data Access

- **API**: SRU 2.0 (Search/Retrieve via URL)
- **Endpoint**: `https://repository.overheid.nl/sru`
- **Full Text**: XML documents at `https://repository.overheid.nl/frbr/officielepublicaties/stb/{year}/{id}/1/xml/{id}.xml`
- **Auth**: None required (Open Government Data)
- **License**: [CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/) (Public Domain)

## Document Types

The Staatsblad contains:
- **Wet** (Law) - Parliamentary legislation
- **AMvB** (Algemene Maatregel van Bestuur) - General Administrative Orders
- **Klein Koninklijk Besluit** - Small Royal Decrees
- Other official decrees and regulations

## Coverage

- ~49,000+ Staatsblad publications
- Historical coverage from 1995 onwards (digitized)
- Full text available in XML format

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (caution: 49K+ records)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Technical Details

### SRU Query

The source uses CQL queries via SRU 2.0:
- `w.publicatienaam==Staatsblad` - Filter for Staatsblad publications
- `dt.modified>=YYYY-MM-DD` - Filter by modification date

### XML Structure

Staatsblad XML documents follow the KOOP schema with elements:
- `<intitule>` - Document title/description
- `<considerans>` - Preamble with legal basis
- `<wettekst>` - Law text body
- `<artikel>` - Individual articles
- `<lid>` - Paragraphs within articles
- `<al>` - Text paragraphs (alinea)
- `<nota-toelichting>` - Explanatory memorandum

## License

[CC0 1.0 Universal](https://creativecommons.org/publicdomain/zero/1.0/) — Public Domain, Dutch Open Government Data.

## Links

- [Staatsblad Search](https://zoek.officielebekendmakingen.nl/uitgebreidzoeken/staatsblad)
- [Wetten.overheid.nl](https://wetten.overheid.nl)
- [Data.overheid.nl - BWB Dataset](https://data.overheid.nl/dataset/basis-wetten-bestand)
- [KOOP Technical Standards](https://standaarden.overheid.nl/)
