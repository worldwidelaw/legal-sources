# DE/OpenLegalData - German Legal Data Platform

## Overview

Open Legal Data (openlegaldata.io) is a non-profit project providing free access to
German legal data. It aggregates court decisions and legislation from all 16 German
states (Länder) plus federal courts.

## Data Coverage

### Court Decisions (Case Law)
- **251,000+** court decisions
- All 16 German states
- Federal courts
- Court levels: Amtsgerichte, Landgerichte, Oberlandesgerichte, Sozialgerichte,
  Arbeitsgerichte, Verwaltungsgerichte, and more

### Legislation
- **57,000+** law texts
- Federal and state laws
- Regulations and ordinances

## API

Base URL: `https://de.openlegaldata.io/api/`

### Endpoints

| Endpoint | Description |
|----------|-------------|
| `/cases/` | Court decisions |
| `/laws/` | Legislation texts |
| `/courts/` | Court information |
| `/states/` | German states (Länder) |

### Query Parameters

- `format=json` - Response format (required)
- `limit=100` - Results per page
- `offset=0` - Pagination offset
- `court__state={id}` - Filter cases by state ID

### State IDs

| ID | State |
|----|-------|
| 3 | Baden-Württemberg |
| 4 | Bayern |
| 5 | Berlin |
| 6 | Brandenburg |
| 7 | Bremen |
| 8 | Hamburg |
| 9 | Hessen |
| 10 | Mecklenburg-Vorpommern |
| 11 | Niedersachsen |
| 12 | Nordrhein-Westfalen |
| 13 | Rheinland-Pfalz |
| 14 | Saarland |
| 15 | Sachsen |
| 16 | Sachsen-Anhalt |
| 17 | Schleswig-Holstein |
| 18 | Thüringen |
| 2 | Bundesrepublik Deutschland (Federal) |

## Authentication

No authentication required. The API is publicly accessible.

## Usage

```bash
# Fetch sample records
python bootstrap.py bootstrap --sample --count 15

# Check API status
python bootstrap.py status

# Fetch cases from Bayern only
python bootstrap.py bootstrap --state 4 --sample
```

## License

German court decisions are public domain under § 5 UrhG (amtliche Werke).

## Links

- Website: https://de.openlegaldata.io/
- API Documentation: https://de.openlegaldata.io/api/
- GitHub: https://github.com/openlegaldata/oldp
