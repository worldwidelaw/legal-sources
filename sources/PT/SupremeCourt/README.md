# PT/SupremeCourt — Portuguese Supreme Court Case Law

**Supremo Tribunal de Justiça (STJ)**

## Overview

This source fetches case law from the Portuguese Supreme Court of Justice (Supremo Tribunal de Justiça - STJ), the highest court for civil and criminal matters in Portugal.

## Data Source

- **Portal**: https://juris.stj.pt
- **Backend**: ElasticSearch-based search with Next.js frontend
- **Original data**: DGSI (dgsi.pt/jstj.nsf)
- **Coverage**: ~71,500 decisions from 1900 to present
- **License**: Public (open government data)

## API Endpoints

### Search API
```
GET https://juris.stj.pt/api/search?MinAno=YYYY&MaxAno=YYYY&mustHaveText=true
```
Returns JSON array of decisions with metadata (but NOT full text).

### Document API
```
GET https://juris.stj.pt/{processo_number}/{uuid_prefix}
```
Returns HTML page with full document data in `__NEXT_DATA__` JSON, including:
- `Texto`: Full decision text (HTML)
- `Sumário`: Summary (HTML)
- Metadata: date, rapporteur, section, area, keywords, etc.

## Data Fields

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (e.g., `STJ-210-24.5YRCBR.S1`) |
| `case_number` | Processo number (e.g., `210/24.5YRCBR.S1`) |
| `text` | Full decision text (cleaned from HTML) |
| `summary` | Decision summary |
| `date` | Decision date (ISO 8601) |
| `rapporteur` | Judge rapporteur name |
| `section` | Court section (e.g., `5.ª Secção (Criminal)`) |
| `area` | Legal area (e.g., `Área Criminal`, `Área Cível`) |
| `procedural_type` | Type of proceeding (e.g., `RECURSO PENAL`, `HABEAS CORPUS`) |
| `outcome` | Decision outcome (e.g., `NEGADO PROVIMENTO`, `PROVIDO`) |
| `keywords` | Legal descriptors/keywords |
| `ecli` | ECLI identifier (when available) |
| `uuid` | Portal internal UUID |

## Types of Cases

- **Criminal Appeals** (Recurso Penal)
- **Civil Appeals**
- **Habeas Corpus**
- **Extradition / European Arrest Warrant**
- **Jurisprudence Fixation** (Recurso de Fixação de Jurisprudência)
- **Jurisdictional Conflicts**
- **Special Court Proceedings**

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (12 decisions with full text)
python bootstrap.py bootstrap --sample

# Full bootstrap (all ~71,500 decisions)
python bootstrap.py bootstrap

# Incremental update (recent decisions)
python bootstrap.py update
```

## Technical Notes

- The search API does not return full text to reduce payload size
- Full text must be fetched via the document endpoint for each decision
- Document endpoint uses Next.js SSR; data is in `__NEXT_DATA__` script tag
- HTML content is cleaned to extract plain text
- Rate limiting: 1 request per 1.5 seconds to respect server load

## ECLI

Portuguese court decisions use the European Case Law Identifier (ECLI) format:
```
ECLI:PT:STJ:YYYY:PROCESSO.UUID
```
Example: `ECLI:PT:STJ:2024:210.24.5YRCBR.S1`

## Related Sources

- **PT/ConstitutionalCourt**: Constitutional Court (Tribunal Constitucional)
- **PT/DiarioRepublica**: Portuguese legislation via Diário da República

## GitHub Repository

The juris.stj.pt portal is open source:
https://github.com/stjiris/jurisprudencia
