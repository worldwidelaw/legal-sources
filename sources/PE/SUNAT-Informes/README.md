# PE/SUNAT-Informes

Peru SUNAT Tax Authority Guidance (Informes, Oficios, Cartas)

## Source

- **URL**: https://www.sunat.gob.pe/legislacion/oficios/
- **Type**: doctrine
- **Auth**: none
- **Coverage**: 1996-present, ~40-100 documents per year

## How it works

1. Fetches yearly index pages at `/legislacion/oficios/{YEAR}/indcor.htm`
2. Parses HTML tables to extract document metadata and links
3. Downloads PDF files (2002+) or HTML files (1996-2001)
4. Extracts full text using PyPDF2 for PDFs or HTML stripping

## Document types

- **Informes**: Binding tax doctrine reports
- **Oficios**: Formal official response letters
- **Cartas**: Response letters to taxpayer queries

## Usage

```bash
python bootstrap.py test-api              # Test connectivity
python bootstrap.py bootstrap --sample    # Fetch 15 sample records
python bootstrap.py bootstrap             # Full bootstrap (all years)
python bootstrap.py update 2026-01-01     # Incremental update
```

## License

[Open Government Data](https://www.sunat.gob.pe) — official tax doctrine published by Peru's National Tax Administration (SUNAT).
