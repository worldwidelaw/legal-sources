# DE/Bremen - Bremen State Law (Transparenzportal)

Bremen state legislation from the official Transparenzportal Bremen.

## Data Source

- **URL**: https://www.transparenz.bremen.de
- **Dataset**: [Gesetze und Rechtsverordnungen Bremen](https://www.transparenz.bremen.de/daten/gesetze-und-rechtsverordnungen-bremen-8261)
- **Format**: XML feed (metadata) + HTML detail pages (full text)
- **License**: [CC BY 3.0](https://creativecommons.org/licenses/by/3.0/)
- **Attribution**: Senator für Finanzen, Bremen

## Data Types

- Laws (Gesetze)
- Regulations (Rechtsverordnungen)
- Ordinances (Satzungen)
- Directives (Verfügungen)

## Access Method

1. Fetch XML export containing metadata for all legislation
2. Parse XML to extract document metadata and detail page URLs
3. Fetch each detail page to extract full text
4. Normalize to standard schema

## Usage

```bash
# Test mode (fetch 3 documents)
python3 bootstrap.py

# Bootstrap sample data (12 documents)
python3 bootstrap.py bootstrap --sample

# Full bootstrap (50 documents)
python3 bootstrap.py bootstrap
```

## License

[CC BY 3.0](https://creativecommons.org/licenses/by/3.0/) — attribution required.

## Notes

- XML feed URL: https://www.transparenz.bremen.de/sixcms/detail.php?template=30_export_template_ifg_d&dt=Gesetze+und+Rechtsverordnungen
- Some entries link to PDFs on external domains (justiz.bremen.de) - these are skipped as full text extraction from PDFs is not implemented
- Rate limiting: 2 requests/second to respect server resources
