# FR/legifrance - French Court of Cassation Case Law

## Overview

This source fetches French case law decisions from the Court of Cassation (Cour de cassation) via DILA's open data bulk archives.

**Data Provider:** DILA / French Prime Minister's Office
**Data URL:** https://echanges.dila.gouv.fr/OPENDATA/CASS/
**License:** Licence Ouverte / Open Licence 2.0
**Update Frequency:** Weekly

## Data Access

DILA provides case law data through weekly tar.gz archives:

- **Format:** `CASS_YYYYMMDD-HHMMSS.tar.gz`
- **Size:** Typically 50-400 KB per archive
- **Content:** Court of Cassation decisions in XML format

No authentication required.

## Court of Cassation

The Cour de cassation is the highest court in the French judicial system. It:
- Reviews appeals on points of law (not facts)
- Can reject appeals (rejet) or overturn (cassation) lower court decisions
- Issues decisions through multiple chambers:
  - Civil chambers (1re, 2e, 3e chambre civile)
  - Commercial chamber (chambre commerciale)
  - Social chamber (chambre sociale)
  - Criminal chamber (chambre criminelle)
  - Mixed chamber (chambre mixte)
  - Plenary assembly (assemblée plénière)

## XML Structure

Each JURITEXT file contains:

```xml
<TEXTE_JURI_JUDI>
  <META>
    <META_COMMUN>
      <ID>JURITEXT000052439669</ID>
      <NATURE>ARRET</NATURE>
    </META_COMMUN>
    <META_SPEC>
      <META_JURI>
        <TITRE>Cour de cassation, civile, Chambre civile 1, 15 octobre 2025...</TITRE>
        <DATE_DEC>2025-10-15</DATE_DEC>
        <JURIDICTION>Cour de cassation</JURIDICTION>
        <SOLUTION>Cassation partielle</SOLUTION>
      </META_JURI>
      <META_JURI_JUDI>
        <NUMEROS_AFFAIRES>
          <NUMERO_AFFAIRE>24-10782</NUMERO_AFFAIRE>
        </NUMEROS_AFFAIRES>
        <FORMATION>CHAMBRE_CIVILE_1</FORMATION>
        <ECLI>ECLI:FR:CCASS:2025:C100661</ECLI>
        <PRESIDENT>Mme Champalaune</PRESIDENT>
      </META_JURI_JUDI>
    </META_SPEC>
  </META>
  <TEXTE>
    <BLOC_TEXTUEL>
      <CONTENU>Full decision text...</CONTENU>
    </BLOC_TEXTUEL>
    <SOMMAIRE>
      <SCT TYPE="PRINCIPAL">Legal headnote...</SCT>
      <ANA>Legal analysis...</ANA>
    </SOMMAIRE>
  </TEXTE>
</TEXTE_JURI_JUDI>
```

## Usage

```bash
# Fetch sample records (15 decisions)
python bootstrap.py bootstrap --sample

# Fetch more samples
python bootstrap.py bootstrap --sample --count 30

# Fetch updates since a date
python bootstrap.py updates --since 2026-02-01
```

## Normalized Schema

Each record contains:

- `_id`: JURITEXT identifier
- `_source`: "FR/legifrance"
- `_type`: "case_law"
- `title`: Decision title (court, chamber, date, case number)
- `text`: Complete decision text
- `date`: Decision date
- `url`: Link to Légifrance
- `ecli`: European Case Law Identifier
- `juridiction`: Court name
- `formation`: Chamber
- `solution`: Outcome (Rejet, Cassation, etc.)
- `numeros_affaires`: Case numbers
- `president`: Presiding judge
- `sommaire`: Legal headnotes
- `citations`: Referenced legal texts

## Related Sources

- **FR/JournalOfficiel**: French legislation (LEGI database)
- **JADE database**: Administrative court decisions (Conseil d'État, etc.)

## Notes

- No authentication required
- TLS connection (HTTPS)
- Recommended rate limit: 1 request/second
- Full decision text extracted from `<BLOC_TEXTUEL>` elements
- HTML tags are stripped from content
- ECLI format: `ECLI:FR:CCASS:YYYY:{chamber}{number}`
