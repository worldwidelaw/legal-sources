# FR/AMF - Autorité des Marchés Financiers

French Financial Markets Authority - Sanctions decisions and regulatory doctrine.

## Data Sources

1. **Sanctions Decisions** (Commission des sanctions)
   - RSS: https://www.amf-france.org/fr/flux-rss/display/24
   - Enforcement actions, fines, and settlements
   - Document IDs: SAN-YYYY-NN format

2. **Regulatory Doctrine**
   - RSS: https://www.amf-france.org/fr/flux-rss/display/31
   - Positions, recommendations, instructions
   - Document IDs: DOC-YYYY-NN format

## Full Text Access

Full text is extracted from PDF documents:
- PDFs are downloaded from `/sites/institutionnel/files/private/...`
- Text extraction via pdfplumber

## Usage

```bash
# Fetch sample records
python3 bootstrap.py bootstrap --sample --limit 15

# List available documents
python3 bootstrap.py list --feed all
```

## Coverage

- Sanctions decisions: Recent 200 items from RSS (capped by AMF)
- Doctrine: Recent 200 items from RSS
- For historical data, use BDIF: https://bdif.amf-france.org

## License

[Licence Ouverte 2.0 / Open Licence (Etalab)](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) — free reuse with attribution.
