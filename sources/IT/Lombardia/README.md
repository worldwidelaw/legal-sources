# IT/Lombardia — Regional Legislation of Lombardia

Regional legislation (laws and regulations) of the Lombardia Region in Italy.

## Data Source

- **Portal**: [dati.lombardia.it](https://www.dati.lombardia.it/government/CRL-Leggi-Regionali-della-Lombardia/abjw-hhay)
- **Publisher**: Consiglio Regionale della Lombardia
- **License**: CC0 1.0 Universal (Public Domain Dedication)
- **Language**: Italian

## Data Coverage

- **Document types**: Leggi Regionali, Regolamenti Regionali
- **Date range**: 1970 to present
- **Record count**: ~2,658 (as of Feb 2026)

## Technical Implementation

### Data Access

1. **Metadata**: Socrata SODA API at `dati.lombardia.it/resource/abjw-hhay.json`
2. **Full Text**: XML (NIR format) at `normelombardia.consiglio.regione.lombardia.it`

### Fields Available

| Field | Description |
|-------|-------------|
| `ids` | Document ID (e.g., lr002026021000004) |
| `estremi` | Citation format (e.g., "Legge Regionale 10 febbraio 2026 n. 4") |
| `titolo` | Title of the law |
| `data_legge` | Date of the law |
| `numero_legge` | Law number |
| `data_burl` | Publication date in BURL (Bollettino Ufficiale) |
| `numero_burl` | BURL issue number |
| `stato_legge` | Status (vigente, abrogata, etc.) |
| `link_testo_xml` | URL to full text XML |

### XML Format

The full text is provided in NIR (Norme in Rete) XML format, the Italian standard for legislative XML.

## Usage

```bash
# Test connectivity
python bootstrap.py test

# Fetch sample records
python bootstrap.py bootstrap --sample

# Full bootstrap
python bootstrap.py bootstrap

# Incremental update
python bootstrap.py update
```

## License

[CC0 1.0](https://creativecommons.org/publicdomain/zero/1.0/) — public domain, no restrictions.

## Notes

- The XML endpoint uses HTTP (not HTTPS) but provides consistent access
- NIR XML includes structured articles, paragraphs, and legal references
- Rate limiting is recommended (1-2 seconds between requests)
