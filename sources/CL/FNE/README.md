# CL/FNE — Fiscalía Nacional Económica

Chile's national competition enforcement authority. Publishes TDLC decisions,
Supreme Court competition sentencias, and historical Antimonopoly Commission decisions.

## Data Source
- **URL**: https://www.fne.gob.cl/biblioteca/jurisprudencia/
- **Type**: case_law
- **Auth**: None required
- **Language**: Spanish
- **Coverage**: 1974-present (~2,400 decisions)

## Strategy
PHP search endpoints at `/search/*_resultados_single.php?palabra=` return all
records in a single HTML table. Each row links to a PDF. Full text is extracted
from PDFs using `common/pdf_extract`.

## Usage
```bash
python bootstrap.py bootstrap          # Full fetch
python bootstrap.py bootstrap --sample # Sample only (15 records)
python bootstrap.py test               # Connectivity test
```

## License

[Chilean Government Public Domain (Ley 20.285)](https://www.leychile.cl/navegar?idNorma=276363) — official competition enforcement decisions, no restrictions.
