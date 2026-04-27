# HR/SupremeCourt - Croatian Supreme Court Case Law

## Overview

This source fetches case law from the Croatian court decisions portal (odluke.sudovi.hr).
The portal is operated by the Ministry of Justice, Administration and Digital Transformation.

**Primary focus:** Supreme Court (Vrhovni sud Republike Hrvatske) decisions
**Database size:** 883,000+ published decisions across all Croatian courts

## Data Source

- **Portal:** https://odluke.sudovi.hr
- **Operator:** Ministry of Justice, Administration and Digital Transformation (Ministarstvo pravosuđa, uprave i digitalne transformacije)
- **License:** [Open Government Data](https://data.gov.hr/en)
- **Language:** Croatian

## Access Method

The portal provides HTML pages with full text of court decisions.

### Endpoints

| Endpoint | Description |
|----------|-------------|
| `/Document/DisplayList` | Search results page with filters |
| `/Document/View?id={uuid}` | Full document view with text and metadata |
| `/Document/DownloadPDF?id={uuid}` | PDF download |

### Filtering

Documents can be filtered by:
- **Court type:** Vrhovni sud, Visoki sudovi, Županijski sudovi, Općinski sudovi, etc.
- **Date range:** Decision date, finality date, publication date
- **ECLI number:** European Case Law Identifier
- **Subject index (Stvarno kazalo):** Legal concepts hierarchy
- **Law index (Zakonsko kazalo):** Referenced legislation
- **EuroVoc:** EU thesaurus keywords

## Data Schema

Each record contains:

| Field | Description |
|-------|-------------|
| `_id` | Document UUID |
| `decision_number` | Official case reference (e.g., "Gr 1-356/2024-2") |
| `court` | Court name |
| `ecli` | ECLI identifier (e.g., "ECLI:HR:VSRH:2024:3808") |
| `decision_date` | Date of decision |
| `publication_date` | Date published to portal |
| `decision_type` | Presuda (judgment), Rješenje (order), etc. |
| `finality` | Whether decision is final |
| `text` | Full text of the decision |
| `subject_index` | Stvarno kazalo (subject classification) |
| `law_index` | Zakonsko kazalo (legislation references) |
| `eurovoc` | EuroVoc keywords |

## ECLI Format

Croatian ECLI identifiers follow the pattern:
```
ECLI:HR:{COURT}:{YEAR}:{NUMBER}
```

Court codes:
- `VSRH` - Vrhovni sud Republike Hrvatske (Supreme Court)
- `VKS` - Visoki kazneni sud (High Criminal Court)
- `VPS` - Visoki prekršajni sud (High Misdemeanor Court)
- `VTS` - Visoki trgovački sud (High Commercial Court)
- `VUS` - Visoki upravni sud (High Administrative Court)
- County/Municipal courts have their own codes

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

[Open Government Data](https://data.gov.hr/en) — Croatian court decisions are freely reusable under open government data terms.

## Notes

- Rate limit: max 3 requests/second
- Full text is embedded in HTML, requires parsing
- Metadata is in structured HTML elements
- Decision text includes the official formatting (REPUBLIKA HRVATSKA header, court seal placeholder, etc.)
