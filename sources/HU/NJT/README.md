# HU/NJT - Hungarian National Legislation Database

**Source:** Nemzeti Jogszabálytár (National Legislation Database)
**URL:** https://njt.hu
**Country:** Hungary (HU)
**Data Types:** Legislation
**License:** [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Overview

The Nemzeti Jogszabálytár (NJT) is Hungary's official legislation database, maintained by MKIFK (Magyar Közlönykiadó és Igazságügyi Fordítóközpont Zrt. - Hungarian Official Journal Publisher and Translation Center).

Hungary implemented ELI (European Legislation Identifier) as of January 1, 2023.

## Document Types

| Code | Hungarian | English |
|------|-----------|---------|
| TV | Törvény | Law/Act |
| TVR | Törvényerejű rendelet | Statutory decree |
| KR | Kormányrendelet | Government decree |
| MR | Miniszteri rendelet | Ministerial decree |

## Data Access

### Search Endpoint
```
GET /search/{type}:{subtype}:{year}:{number}:{text}:{title}:{effectiveDate}:.../{page}/{size}
```

Example: `/search/-:-:2024:-:-:-:-:-:-:-:1/1/50`

### Document Endpoint
```
GET /jogszabaly/{year}-{number}-{mod1}-{mod2}
```

Example: `/jogszabaly/2024-45-00-00`

### ELI Endpoint
```
GET /eli/{type}/{year}/{number}
```

Example: `/eli/TV/2024/45`

## Implementation Notes

- Full text is server-rendered in HTML (no JavaScript required)
- Text content is in the `<div class="jogszabaly">` element
- Metadata includes effective dates in `<div class="hataly">` elements
- Rate limiting: 1 request/second recommended

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Hungarian national legislation is open government data.

## References

- [NJT Homepage](https://njt.hu)
- [ELI URI Templates](https://njt.hu/eli/urisemak)
- [N-Lex Hungary Info](https://n-lex.europa.eu/n-lex/info/info-hu/index)
- [EUR-Lex ELI Hungary](https://eur-lex.europa.eu/eli-register/hungary.html)
