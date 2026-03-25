# UN/UHRI — Universal Human Rights Index

**Source:** [OHCHR Universal Human Rights Index](https://uhri.ohchr.org/)

## Overview

260,000+ observations and recommendations from three UN human rights mechanisms:
- **Treaty Bodies** (CESCR, CCPR, CRC, CEDAW, CERD, CAT, CMW, CRPD, CED)
- **Special Procedures** (Special Rapporteurs, Working Groups, etc.)
- **Universal Periodic Review** (UPR)

## Data Access

Bulk JSON export from OHCHR's data portal (no authentication required):
- **URL:** `https://dataex.ohchr.org/uhri/export-results/export-full-en.json`
- **Size:** ~360 MB
- **Updated:** Daily

## Record Fields

| Field | Description |
|-------|-------------|
| `AnnotationId` | Unique UUID |
| `Text` | Full text of recommendation/observation (HTML, stripped to plain text) |
| `Symbol` | UN document symbol (e.g., E/C.12/AUS/CO/6) |
| `Body` | UN mechanism (CESCR, CCPR, UPR, etc.) |
| `Countries` | Target country/countries |
| `Themes` | Human rights themes |
| `Sdgs` | Related Sustainable Development Goals |
| `AnnotationType` | Recommendations, Concerns/Observations, etc. |
| `PublicationDate` | Date of publication |

## Usage

```bash
python bootstrap.py test-api             # Test connectivity
python bootstrap.py bootstrap --sample   # Fetch 15 sample records
python bootstrap.py bootstrap            # Full pull (260K+ records)
```

## License

Open data from the UN Office of the High Commissioner for Human Rights.
