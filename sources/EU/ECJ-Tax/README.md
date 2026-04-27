# EU/ECJ-Tax Data Source

European Court of Justice case law related to taxation, VAT, fiscal matters, and excise duties.

## Data Source

- **URL**: https://curia.europa.eu
- **API**: Publications Office SPARQL + CELLAR API
- **Type**: case_law
- **Auth**: None required

## Coverage

This source fetches CJEU judgments and filters them to include only tax-related cases based on:

1. **Directive References**: Cases referencing key EU tax directives:
   - 2006/112/EC (VAT Directive)
   - 77/388/EEC (Sixth VAT Directive)
   - 2008/118/EC (Excise Directive)
   - 2011/96/EU (Parent-Subsidiary Directive)
   - 2003/49/EC (Interest and Royalties Directive)
   - 2016/1164/EU (Anti-Tax Avoidance Directive)
   - And others

2. **Tax Keywords**: Documents containing significant tax-related terminology:
   - VAT, taxation, fiscal, excise
   - Income tax, corporate tax, withholding tax
   - Tax exemption, tax deduction, taxable person
   - Transfer pricing, state aid, double taxation
   - And many more

## Usage

```bash
# Test the fetcher
python3 bootstrap.py

# Fetch sample data (10+ tax cases)
python3 bootstrap.py bootstrap --sample

# Full bootstrap
python3 bootstrap.py bootstrap

# With date range
python3 bootstrap.py bootstrap --start-date=2020-01-01 --end-date=2024-01-01
```

## Output Schema

Each record includes:

| Field | Type | Description |
|-------|------|-------------|
| _id | string | CELEX identifier |
| _source | string | "EU/ECJ-Tax" |
| _type | string | "case_law" |
| celex_id | string | CELEX identifier |
| ecli | string | European Case Law Identifier |
| court | string | Court of Justice or General Court |
| document_type | string | judgment, order, or opinion |
| title | string | Case title |
| text | string | Full text of the judgment |
| date | string | Date of decision (YYYY-MM-DD) |
| url | string | EUR-Lex URL |
| tax_keywords | array | Tax keywords found in text |
| directive_references | array | Tax directives referenced |

## License

[EUR-Lex legal notice](https://eur-lex.europa.eu/content/legal-notice/legal-notice.html) — reuse authorised provided the source is acknowledged.

## Notes

- Uses the same underlying SPARQL + CELLAR API as EU/curia
- Filtering is done client-side after fetching full text
- A case is considered tax-related if it references a tax directive OR contains 3+ tax keywords
