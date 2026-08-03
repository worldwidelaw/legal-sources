# DO/RealEstateRegistry — Registro Inmobiliario / Jurisdicción Inmobiliaria

Full-text adjudicatory decisions of the Dominican Republic's Real Estate
Jurisdiction (Jurisdicción Inmobiliaria), specifically the **Resoluciones sobre
Recursos Jerárquicos** issued by the Dirección Nacional de Registro de Títulos
(DNRT). Each resolution resolves a hierarchical administrative appeal against an
act of the title registry and contains the parties, the facts, the legal
reasoning, and the resolved outcome.

- **Source:** https://ri.gob.do/?page_id=3929
- **Language:** Spanish
- **Coverage:** 2019–present (~1,200 resolutions)
- **Format:** Full-text PDFs (`DNRT-R-YYYY-NNNNN.pdf`), indexed by year/month.

## Access

The decisions are linked from a single WordPress index page and stored as public
PDFs under `ri.gob.do/wp-content/uploads/YYYY/MM/`. The scraper extracts every
`DNRT-R-*.pdf` link from the index, downloads each PDF, and extracts its text with
pdfplumber (with per-page cache flushing to bound memory on large documents).

`ri.gob.do` serves a certificate chain some clients reject, so the scraper
disables TLS verification — these are public official acts.

## Scope note

The Jurisdicción Inmobiliaria's own judicial *sentencias* (Tribunales de Tierras)
are **not** openly browsable: the Unidad de Consulta digital library requires
property identifiers (matrícula, designación catastral) or in-person access. The
DNRT hierarchical-appeal resolutions published at `page_id=3929` are therefore the
openly accessible decision corpus from `ri.gob.do`.

## Usage

```bash
python bootstrap.py test               # connectivity + one-record check
python bootstrap.py bootstrap --sample # 15 sample records
python bootstrap.py bootstrap          # full pull
```

## License

[Public Domain (Government)](https://ri.gob.do/) — official acts of the Dominican
Real Estate Jurisdiction, published under the publicity mandate of **Ley No. 108-05
de Registro Inmobiliario**. Commercial use permitted.
