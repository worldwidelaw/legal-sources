# FR/AMF_Doctrine - AMF Regulatory Doctrine

French financial market authority (Autorité des Marchés Financiers) regulatory doctrine documents.

## Data Source

- **URL**: https://www.amf-france.org/fr/reglementation/doctrine
- **RSS Feed**: https://www.amf-france.org/fr/flux-rss/display/31
- **License**: Licence Ouverte Etalab 2.0

## Document Types

- **Instructions (DOC-YYYY-NN)**: Binding guidance on applying regulations
- **Positions**: AMF interpretations and recommendations
- **Professional Rules (Règles professionnelles approuvées)**: Industry rules approved by AMF

## Coverage

The RSS feed provides access to all AMF doctrine documents that have been created or updated. Each document page contains:

- Full text of the doctrine
- PDF downloads (official versions)
- Publication and update dates
- Related regulations and references

## Usage

```bash
# Fetch sample records for validation
python bootstrap.py bootstrap --sample

# Full bootstrap (all items in RSS feed)
python bootstrap.py bootstrap

# Incremental updates since a date
python bootstrap.py updates --since 2026-01-01
```

## Notes

- Rate limited to 0.5 req/sec to respect server load
- Full text extracted from HTML pages
- PDFs available but not required for text extraction
