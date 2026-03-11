# NL/wetten.overheid.nl - Dutch Consolidated Legislation

## Overview

This source fetches **consolidated Dutch legislation** from the Basis Wetten Bestand (BWB) via the KOOP SRU API.

**Key distinction from NL/Staatsblad:**
- **Staatsblad**: Official gazette publications (as-published amendments)
- **wetten.overheid.nl**: CONSOLIDATED legislation (integrated current law)

Both are valuable - Staatsblad for historical/original publication text, and wetten.overheid.nl for the current, integrated version of laws.

## Data Coverage

- ~45,000 regulations with 100,000+ versions
- Laws (wetten), AMvBs (general administrative orders), ministerial regulations
- All consolidated text since May 2002
- Full text in XML format

## API Details

- **SRU Endpoint**: `https://zoekservice.overheid.nl/sru/Search`
- **Connection**: `BWB` (Basis Wetten Bestand)
- **Protocol**: SRU 1.2 (Search/Retrieve via URL)
- **Full Text**: XML files at `repository.officiele-overheidspublicaties.nl`

### Example SRU Query

```
https://zoekservice.overheid.nl/sru/Search?operation=searchRetrieve&version=1.2&x-connection=BWB&query=dcterms.type==wet&maximumRecords=10
```

### CQL Query Syntax

- `dcterms.type==wet` - Laws only
- `dcterms.type==amvb` - General administrative orders
- `dcterms.modified>=2024-01-01` - Modified since date
- `overheid.authority=="Financiën"` - By responsible ministry

## License

CC0 1.0 Universal (Public Domain) - Dutch Open Government Data

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records with full text
python bootstrap.py bootstrap --sample

# Full bootstrap (45K+ records)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Data Schema

Each normalized record contains:

| Field | Description |
|-------|-------------|
| `_id` | BWB identifier (e.g., BWBR0001826) |
| `_source` | NL/wetten.overheid.nl |
| `_type` | legislation |
| `title` | Official title of the regulation |
| `text` | **Full text** of the consolidated legislation |
| `date` | Effective date |
| `url` | Link to wetten.overheid.nl |
| `doc_type` | wet, amvb, ministerieleregeling, etc. |
| `rechtsgebied` | Legal domain(s) |
| `overheidsdomein` | Government domain(s) |
| `geldig_start` | Validity start date |
| `geldig_end` | Validity end date |

## References

- [wetten.overheid.nl](https://wetten.overheid.nl) - Official portal
- [data.overheid.nl/dataset/basis-wetten-bestand](https://data.overheid.nl/dataset/basis-wetten-bestand) - Dataset documentation
- [SRU Standard](https://www.loc.gov/standards/sru/) - Library of Congress
