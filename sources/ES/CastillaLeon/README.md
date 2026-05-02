# ES/CastillaLeon — Castilla y León Regional Legislation (BOCYL)

Fetches legislation from the Boletín Oficial de Castilla y León (BOCYL) via:
1. OpenDataSoft API for structured metadata and pagination
2. XML endpoints for full text extraction

## Coverage

- **Temporal**: 1983–present
- **Document types**: Leyes, Decretos, Decretos Legislativos, Decretos-ley, Órdenes, Resoluciones, Acuerdos
- **Sections**: Disposiciones Generales, Disposiciones y Actos, Otras Disposiciones
- **Language**: Spanish (es)
- **Estimated records**: ~15,000+

## Usage

```bash
python bootstrap.py bootstrap          # Full initial pull
python bootstrap.py bootstrap --sample # Fetch 12 sample records
python bootstrap.py update             # Incremental update
python bootstrap.py test               # Quick connectivity test
```

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — Attribution required.
