# IT/CassazioneCivile - Italian Court of Cassation

## Overview

This data source fetches case law from Italy's Supreme Court (Corte Suprema di Cassazione) via the SentenzeWeb Solr API.

## Data Access

**API**: Solr REST API at `https://www.italgiure.giustizia.it/sncass/isapi/hc.dll/sn.solr/sn-collection/select`

**Authentication**: None required (public access)

**Coverage**:
- 1.87M+ total documents
- 186K+ civil decisions (snciv)
- 237K+ criminal decisions (snpen)
- Rolling 5+ years of decisions

## Full Text

Full text is available in the `ocr` field - extracted via OCR from the original PDF decisions. The text quality is generally good, though some OCR artifacts may be present.

## Document Types

| Kind | Description |
|------|-------------|
| `snciv` | Civil Section decisions |
| `snpen` | Criminal Section decisions |
| `sic` | Civil/Criminal registry entries |

## Key Fields

| Field | Description | Example |
|-------|-------------|---------|
| `id` | Unique document ID | `snciv2021521018O` |
| `ocr` | Full text (OCR) | [full decision text] |
| `numdec` | Decision number | `21018` |
| `anno` | Year | `2021` |
| `tipoprov` | Decision type | `Sentenza`, `Ordinanza` |
| `szdec` | Section number | `5` |
| `presidente` | President judge | `CIRILLO ETTORE` |
| `relatore` | Reporting judge | `ROSSI RAFFAELE` |
| `materia` | Subject matter | `IRPEF ILOR ACCERTAMENTO` |
| `datdep` | Deposit date | `20210722` |
| `datdec` | Decision date | `20210512` |

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records (12)
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

Italian Open Data License (IODL) 2.0 - Commercial use permitted with attribution.

## Notes

- SSL certificate verification is disabled due to certificate issues on italgiure.giustizia.it
- The API is rate-limited; the scraper uses 1.5s delays between requests
- ECLI identifiers are generated for European case law interoperability
