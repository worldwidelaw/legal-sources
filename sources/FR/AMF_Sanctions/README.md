# FR/AMF_Sanctions - AMF Enforcement Committee Decisions

French financial market authority (Autorité des Marchés Financiers) enforcement committee decisions.

## Data Source

- **URL**: https://www.amf-france.org/fr/sanctions-transactions/decisions-de-la-commission-des-sanctions
- **RSS Feed**: https://www.amf-france.org/fr/flux-rss/display/23 (filtered for sanctions)
- **License**: Licence Ouverte Etalab 2.0

## Decision Types

The Commission des sanctions handles enforcement actions for:

- **Market abuse**: Insider dealing, market manipulation
- **Professional obligation breaches**: Investment services providers
- **Information disclosure violations**: Listed companies, issuers
- **Collective investment misconduct**: Asset management companies

## Coverage

Decisions are filtered from the AMF press releases RSS feed. Each decision page contains:

- Full text of the decision announcement
- PDF downloads (official decision documents)
- Sanction amounts
- Publication dates

## Usage

```bash
# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all sanctions in RSS feed)
python bootstrap.py bootstrap

# Incremental updates since a date
python bootstrap.py updates --since 2026-01-01
```

## Notes

- Filters RSS feed for "communiques-de-la-commission-des-sanctions" URLs only
- Rate limited to 0.5 req/sec to respect server load
- Decision numbers follow SAN-YYYY-NN format
- Full decision PDFs usually available on page

## License

[Licence Ouverte 2.0 / Open Licence (Etalab)](https://www.etalab.gouv.fr/licence-ouverte-open-licence/) — free reuse with attribution.
