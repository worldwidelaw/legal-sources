# AT/BWB - Austrian Competition Authority Decisions

## Overview

This source fetches cartel and competition law decisions from the Austrian Legal Information System (RIS).

The **Bundeswettbewerbsbehörde (BWB)** - Austrian Federal Competition Authority - is the investigative body for competition law matters in Austria. However, actual decisions are made by:

- **Kartellgericht** (OLG Wien acting as Cartel Court) - First instance
- **Kartellobergericht** (OGH acting as Supreme Cartel Court) - Appellate instance

## Data Source

- **API**: RIS OGD API v2.6 (Open Government Data)
- **Endpoint**: `https://data.bka.gv.at/ris/api/v2.6/Judikatur`
- **Documentation**: [RIS API Documentation](https://data.bka.gv.at/ris/ogd/v2.6/Documents/Dokumentation_OGD-RIS_API.pdf)

## Search Strategy

The scraper searches for decisions containing:
- "Kartellgericht"
- "Kartellobergericht"
- "Bundeswettbewerbsbehörde"
- "Wettbewerbsrecht"

This captures decisions under:
- **KartG** (Kartellgesetz - Cartel Act 2005)
- **WettbG** (Wettbewerbsgesetz - Competition Act)

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (10+)
python bootstrap.py bootstrap --sample

# Full bootstrap (all records)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## Output Schema

| Field | Description |
|-------|-------------|
| `_id` | RIS document ID |
| `_source` | "AT/BWB" |
| `_type` | "kartellgericht_decision", "kartellobergericht_decision", or "regulatory_decision" |
| `title` | Case number (Geschäftszahl) |
| `text` | Full text of the decision |
| `date` | Decision date |
| `url` | Link to RIS document |
| `ecli` | European Case Law Identifier |
| `geschaeftszahl` | Austrian case number |
| `normen` | Referenced legal norms |
| `gericht` | Court name |

## Case Number Patterns

- `16Ok1/26x` - OGH as Kartellobergericht (16th Senate, Ok = Oberster Kartellobergericht)
- `27Kt5/18i` - OLG Wien as Kartellgericht (27th Senate, Kt = Kartell)

## Legal Framework

- **Kartellgesetz 2005 (KartG)** - Austrian Cartel Act
- **Wettbewerbsgesetz (WettbG)** - Austrian Competition Act
- **§ 37 KartG** - Publication requirements for cartel court decisions

## Notes

- Decisions are published in the **Ediktsdatei** (official gazette) under § 37 KartG
- Full text is available via RIS in XML, HTML, RTF, and PDF formats
- Most decisions are in German

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Austrian Open Government Data (RIS OGD API).
