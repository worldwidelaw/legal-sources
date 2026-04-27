# MY/FederalLegislation — Laws of Malaysia Online

**Source:** Attorney General's Chambers (AGC) of Malaysia
**URL:** https://lom.agc.gov.my/
**Type:** legislation
**Coverage:** 878+ federal principal acts (bilingual English/Malay)

## Strategy

1. List all acts via the DataTable JSON endpoint (`json-updated-2024.php`)
2. Parse English PDF download URLs from the JSON response
3. Download PDFs and extract full text using PyPDF2

## Data Access

- JSON endpoint returns act metadata including PDF paths
- PDFs are hosted at `lom.agc.gov.my/ilims/upload/portal/akta/outputaktap/`
- No authentication required
- Rate limited to 0.5 req/s

## Usage

```bash
python bootstrap.py test-api             # Connectivity test
python bootstrap.py bootstrap --sample   # Fetch 15 sample records
python bootstrap.py bootstrap            # Full fetch (878+ acts)
```

## License

[Open Government Data](https://lom.agc.gov.my/) — official federal legislation published by the Attorney General's Chambers of Malaysia.

> **Note:** Re-use may be subject to Malaysian government copyright terms. See [AGC website](https://lom.agc.gov.my/) for details.
