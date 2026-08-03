# MX/PoderJudicialMorelos — Morelos State Court Decisions

Full-text court decisions (*sentencias*, versiones públicas) from the
**Poder Judicial del Estado de Morelos**, Mexico.

## Source

- Portal: <https://tsjmorelos.gob.mx/sentencias/>
  ("Buscador Temático de Versiones Públicas de Sentencias", SISEMOR)
- Angular front-end: <http://sisemor.tsjmorelos2.gob.mx:8000/sentencias>
- API base: `http://sisemor.tsjmorelos2.gob.mx:8080/`
- Type: `case_law` (state judiciary, ISO 3166-2 **MX-MOR**)

## How it works

The SISEMOR back-end exposes a plain JSON API (no auth):

1. `GET buscador/listarEstadistica` → list of **all** ~32,800 public sentencias,
   each with `id_sentencia`, `numero_expediente`, `fecha_sentencia`,
   `id_materia`, `id_juzgado`, `id_distrito`, `sede`, etc.
2. `GET sentencia/verDocumento?idSentencia=<id>` → JSON with `nombre_documento`.
3. `GET archivos/<nombre_documento>` → the full-text PDF.

Catalog endpoints (`catalogos/materias`, `catalogos/tipojuzgado`,
`catalogos/distritos`, `catalogos/resolucion`) map numeric ids to
human-readable labels. The scraper iterates newest-first, downloads each PDF,
and extracts its text.

## Usage

```bash
python bootstrap.py test                    # connectivity + sample PDF check
python bootstrap.py bootstrap --sample      # save ~12 sample records
python bootstrap.py bootstrap-fast --full   # full run, streams to data/records.jsonl
```

## License

[Términos de Libre Uso de la Información (LIBRE USO MX)](https://datos.gob.mx/libreusomx) — Mexican government public information, freely reusable; commercial use permitted with attribution.

Public-version court decisions are published by the Morelos state judiciary as a
transparency obligation (versiones públicas).
