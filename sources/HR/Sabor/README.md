# HR/Sabor -- Croatian Parliament Debate Transcripts

**Source:** edoc.sabor.hr (Croatian Parliament e-document system)
**Type:** Doctrine (parliamentary debate transcripts)
**Auth:** None (open access)
**Language:** Croatian (HRV)

## Overview

Fetches full-text parliamentary debate transcripts (fonogrami) from the Croatian
Parliament (Hrvatski sabor). Each record is one agenda item debate with
speaker-attributed text including party affiliation.

## Endpoints

- **Grid listing:** `https://edoc.sabor.hr/Fonogrami.aspx`
- **Fonogram view:** `https://edoc.sabor.hr/Views/FonogramView.aspx?tdrid={id}`

## Data

- ~740 debate transcripts across multiple parliamentary terms (sazivi)
- Speaker entries: `Surname, Name (PARTY)` with full speech text
- Text ranges from ~500 to ~435,000 characters per debate

## Usage

```bash
python bootstrap.py test               # Quick connectivity test
python bootstrap.py bootstrap --sample # Fetch 10+ sample records
python bootstrap.py bootstrap          # Full initial pull
python bootstrap.py update             # Incremental update
```
