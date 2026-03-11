# FR/JournalOfficiel - French Official Gazette (LEGI Database)

## Overview

This source fetches French consolidated legislation from the DILA (Direction de l'information légale et administrative) open data bulk archives.

**Data Provider:** DILA / French Prime Minister's Office
**Data URL:** https://echanges.dila.gouv.fr/OPENDATA/LEGI/
**License:** Licence Ouverte / Open Licence 2.0
**Update Frequency:** Daily

## Data Access

DILA provides legislation data through:

1. **Daily Incremental Archives** (1-10 MB tar.gz files)
   - Format: `LEGI_YYYYMMDD-HHMMSS.tar.gz`
   - Contains only changes since the previous day

2. **Full Database Archive** (~1 GB)
   - Format: `Freemium_legi_global_*.tar.gz`
   - Complete consolidated legislation database

This implementation uses the daily incremental archives for efficiency.

## XML Structure

Each archive contains two types of XML files:

### LEGITEXT (Text Container)
```xml
<TEXTE_VERSION>
  <META>
    <ID>LEGITEXT000033899300</ID>
    <NATURE>ARRETE</NATURE>
    <CID>JORFTEXT000033893593</CID>
    <NOR>RDFF1634959A</NOR>
    <TITRE>Arrêté du 10 janvier 2017</TITRE>
    <TITREFULL>Arrêté du 10 janvier 2017 pris pour...</TITREFULL>
    <ETAT>ABROGE</ETAT>
    <DATE_PUBLI>2017-01-20</DATE_PUBLI>
  </META>
  <VISAS><CONTENU>...</CONTENU></VISAS>
  <SIGNATAIRES><CONTENU>...</CONTENU></SIGNATAIRES>
</TEXTE_VERSION>
```

### LEGIARTI (Article Content)
```xml
<ARTICLE>
  <META>
    <ID>LEGIARTI000033839813</ID>
    <NUM>5</NUM>
    <ETAT>MODIFIE</ETAT>
  </META>
  <BLOC_TEXTUEL>
    <CONTENU>
      <p>Full article text here...</p>
    </CONTENU>
  </BLOC_TEXTUEL>
</ARTICLE>
```

## Usage

```bash
# Fetch sample records (15 documents)
python bootstrap.py bootstrap --sample

# Fetch more samples
python bootstrap.py bootstrap --sample --count 30

# Fetch updates since a date
python bootstrap.py updates --since 2026-02-01
```

## Document Types

- **LOI** - Laws
- **ORDONNANCE** - Ordinances
- **DECRET** - Decrees
- **ARRETE** - Ministerial orders
- **CODE** - Legal codes

## Normalized Schema

Each record contains:
- `_id`: LEGITEXT identifier
- `_source`: "FR/JournalOfficiel"
- `_type`: "legislation"
- `title`: Full document title
- `text`: Complete document text (visas + articles + signataires)
- `date`: Publication date
- `url`: Link to Légifrance
- `nature`: Document type (LOI, DECRET, etc.)
- `nor`: NOR identifier
- `etat`: Status (VIGUEUR, ABROGE, MODIFIE)

## Notes

- No authentication required
- TLS connection (HTTPS)
- Recommended rate limit: 1 request/second
- Full text is extracted from LEGIARTI `<BLOC_TEXTUEL>` elements
- HTML tags are stripped from content
