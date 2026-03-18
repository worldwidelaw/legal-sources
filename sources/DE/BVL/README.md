# DE/BVL - German Federal Office for Consumer Protection (BVL)

## Overview

This source fetches plant protection product (pesticide) authorization data from the Bundesamt für Verbraucherschutz und Lebensmittelsicherheit (BVL).

**URL**: https://psm-api.bvl.bund.de/

## Data Type

- `regulatory_decisions`: Plant protection product authorizations

## API Details

The BVL provides a REST API (PSM-API) with OpenAPI/Swagger documentation:

- Base URL: `https://psm-api.bvl.bund.de/ords/psm/api-v1/`
- Format: JSON
- Authentication: None required (public API)
- Rate Limits: No explicit limits, but courtesy delays are applied

### Main Endpoints Used

| Endpoint | Description |
|----------|-------------|
| `/mittel/` | Approved plant protection products |
| `/wirkstoff/` | Active ingredients |
| `/wirkstoff_gehalt/` | Ingredient concentrations per product |
| `/awg/` | Approved applications (crops, pests, conditions) |
| `/auflagen/` | Regulatory requirements and conditions |
| `/kode/` | Code definitions for decoding |
| `/ghs_gefahrenhinweise/` | GHS hazard statements |
| `/ghs_sicherheitshinweise/` | GHS precautionary statements |
| `/stand/` | Database update date |

## Full Text Construction

Since the API provides structured regulatory data rather than documents, full text is constructed by combining:

1. Product identification (name, authorization number, dates)
2. Active ingredients with concentrations
3. Approved applications (limited to 20 per product)
4. Regulatory requirements and conditions (decoded)
5. GHS hazard and safety information (decoded)

## Data Volume

- ~2,100 currently approved products
- Data updated monthly by BVL

## Usage

```bash
# Test mode (3 documents)
python3 bootstrap.py

# Bootstrap with samples (15 documents)
python3 bootstrap.py bootstrap --sample

# Full bootstrap (100 documents)
python3 bootstrap.py bootstrap
```

## License

Public domain - Official German government data (§ 5 UrhG).

## Links

- [BVL Website](https://www.bvl.bund.de/)
- [PSM-API Documentation](https://psm-api.bvl.bund.de/)
- [Online Database](https://www.bvl.bund.de/DE/Arbeitsbereiche/04_Pflanzenschutzmittel/01_Aufgaben/02_ZulassungPSM/01_ZugelPSM/01_OnlineDatenbank/psm_onlineDB_node.html)
