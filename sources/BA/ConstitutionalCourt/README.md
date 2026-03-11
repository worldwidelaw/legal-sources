# BA/ConstitutionalCourt - Constitutional Court of Bosnia and Herzegovina

## Overview

This source fetches case law decisions from the Constitutional Court of Bosnia and Herzegovina (Ustavni sud Bosne i Hercegovine).

- **Website**: https://www.ustavnisud.ba
- **Data type**: Case law (constitutional court decisions)
- **Languages**: Bosnian (bs), Croatian (hr), Serbian (sr), English (en)
- **License**: Public domain (official court decisions)

## Data Access

The court provides a REST API for searching decisions:

- **API Base**: `https://www.ustavnisud.ba/bs/api/odluke`
- **Full text**: Available as PDF files at `/uploads/odluke/{filename}.pdf`

### API Parameters

| Parameter | Description | Example |
|-----------|-------------|---------|
| `fp` | Predefined filter (1 = latest session) | `fp=1` |
| `sp` | Sort order | `DatumDesc`, `DatumAsc` |
| `tp` | Text search | Free text |
| `bp` | Case number | `AP-1234/21` |
| `vo[]` | Decision type IDs | `vo[]=2` |
| `vp[]` | Case type IDs | `vp[]=1` |

**Note**: The API requires the `fp=1` parameter to return results. Queries without filters return 500 errors due to server memory limitations.

## Decision Types

| ID | Bosnian | English |
|----|---------|---------|
| 1 | Odluka o dopustivosti | Admissibility decision |
| 2 | Odluka o meritumu | Merit decision |
| 3 | Odluka o privremenoj mjeri | Interim measure decision |
| 4 | Rješenje, zaključak, obavještenje | Ruling, conclusion, notice |
| 5 | Rješenje o neizvršenju | Non-compliance ruling |
| 6 | Odluka o obustavi postupka | Discontinuation decision |

## Case Types

| ID | Code | Description |
|----|------|-------------|
| 1 | AP | Appeals (Apelacije) |
| 2 | U | Abstract constitutional review (Ustavnost) |

## Usage

```bash
# Fetch sample data (12 records with full text)
python3 bootstrap.py bootstrap --sample

# Test API connectivity
python3 bootstrap.py test

# Fetch specific number of records
python3 bootstrap.py bootstrap --sample --limit 20

# Fetch without PDF text extraction (metadata only)
python3 bootstrap.py bootstrap --sample --no-text
```

## Output Schema

Each record contains:

| Field | Description |
|-------|-------------|
| `_id` | Unique identifier (e.g., `BA-CC-AP-1234-21-123456`) |
| `_source` | Source identifier (`BA/ConstitutionalCourt`) |
| `_type` | Data type (`case_law`) |
| `case_number` | Official case number (e.g., `AP-1234/21`) |
| `title` | Case title with appellant name |
| `text` | **Full text** extracted from PDF |
| `conclusion` | Court's conclusion/summary |
| `date` | Decision date (ISO 8601) |
| `url` | Link to PDF document |
| `decision_type` | Type of decision |
| `case_type` | Type of case (AP or U) |
| `disputed_act` | The act being challenged |
| `keywords` | Subject matter keywords |
| `violations_found` | Rights found to be violated |
| `no_violations_found` | Rights found not to be violated |

## Dependencies

- `requests` - HTTP client
- `pdfplumber` or `PyPDF2` - PDF text extraction

Install with:
```bash
pip install requests pdfplumber
```

## Notes

1. The court website is at `ustavnisud.ba` (not `ccbh.ba` which shows a parking page)
2. Decisions are published in three official languages of Bosnia and Herzegovina
3. The API returns 270 items from the latest session filter
4. Full text is extracted from PDF documents, typically 5,000-50,000 characters per decision
5. Some appellants' names are anonymized (initials only) per court policy
