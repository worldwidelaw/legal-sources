# AT/VwGH - Austrian Supreme Administrative Court

## Overview

The Austrian Supreme Administrative Court (Verwaltungsgerichtshof, VwGH) is Austria's
highest court for administrative matters. It reviews decisions of administrative courts
and authorities on issues such as taxation, asylum, social welfare, public procurement,
and regulatory compliance.

## Data Source

**API:** RIS OGD API v2.6
**Endpoint:** https://data.bka.gv.at/ris/api/v2.6/Judikatur
**Application:** Vwgh
**Records:** ~354,000+
**License:** CC BY 4.0

## Coverage

- Administrative law decisions since 1990s
- Tax cases (Abgaben)
- Asylum/migration cases (Fremdenrecht)
- Social welfare cases (Sozialrecht)
- Public procurement cases (Vergaberecht)
- Environmental cases (Umweltrecht)
- Building/planning cases (Baurecht)

## Full Text Access

Full text is obtained via:
1. XML content URLs (cleanest extraction)
2. HTML content URLs (fallback)
3. Linked decision text documents (Entscheidungstexte)
4. Rechtssatz (legal principle summaries) as last resort

## Usage

```bash
# Test API connectivity
python bootstrap.py test-api

# Fetch sample records (12 records for validation)
python bootstrap.py bootstrap --sample

# Full bootstrap (354K+ records - will take many hours)
python bootstrap.py bootstrap

# Incremental update (since last sync)
python bootstrap.py update
```

## Schema

| Field | Description |
|-------|-------------|
| `_id` | RIS document ID |
| `ecli` | European Case Law Identifier |
| `geschaeftszahl` | Case number |
| `title` | Case title |
| `text` | Full text of the decision |
| `date` | Decision date |
| `url` | Link to RIS portal |
| `entscheidungsart` | Decision type (Erkenntnis, Beschluss) |
| `normen` | Referenced legal norms |
| `schlagworte` | Keywords |
| `rechtssatz` | Legal principle summary |

## Notes

- Uses the same RIS OGD API as AT/VfGH (Constitutional Court) and AT/OGH (Supreme Court)
- Austria's largest case law database by volume
- Rate limited to 1 request per 2 seconds for bulk operations
- Full text is MANDATORY - metadata-only records are rejected
