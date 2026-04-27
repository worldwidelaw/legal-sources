# BE/CourCassation-Fiscal - Belgian Court of Cassation Tax Cases

Belgian Court of Cassation (Cour de Cassation / Hof van Cassatie) tax/fiscal case law fetcher.

## Data Source

- **Website**: https://juportal.be
- **Type**: Case Law (Tax/Fiscal matters)
- **Coverage**: Tax cases from Court of Cassation
- **Languages**: French, Dutch, German
- **License**: Open Government Data

## How It Works

This source filters Court of Cassation decisions for tax/fiscal matters based on:

1. **Role number prefix "F."** - Fiscal chamber cases
2. **Subject/keyword matching** - Cases tagged with fiscal terms like:
   - French: impôt, taxe, TVA, contribution, fiscal
   - Dutch: belasting, BTW, fiscaal, heffing
   - German: Steuer, Mehrwertsteuer, Abgabe

## Usage

```bash
# Run quick connectivity test
python bootstrap.py test

# Fetch 10+ sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all fiscal cases)
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update

# Check checkpoint status
python bootstrap.py status
```

## Data Schema

Each record includes:

| Field | Description |
|-------|-------------|
| `_id` | ECLI identifier |
| `_source` | `BE/CourCassation-Fiscal` |
| `_type` | `case_law` |
| `title` | Case name |
| `text` | **Full text of the decision** |
| `date` | Decision date (ISO 8601) |
| `url` | Link to JUPORTAL page |
| `ecli` | European Case Law Identifier |
| `court` | Court name |
| `chamber` | Chamber code (e.g., 1F, 2N) |
| `language` | fr, nl, or de |
| `role_number` | Case roll number (e.g., F.21.0118.N) |
| `abstract` | Summary/headnote |
| `subjects` | Subject classification |
| `keywords` | Thesaurus keywords |

## Technical Notes

- Uses ECLI sitemaps from robots.txt for document discovery
- Sitemap XML contains rich metadata (subjects, keywords, abstract)
- Full text fetched from /content/ECLI:... endpoint
- Rate limited to 1.5 seconds between requests
- Supports checkpoint/resume for large fetches

## License

[Belgian Open Government Data](https://data.gov.be/en/licence-conditions) — free reuse of Belgian public sector information.
