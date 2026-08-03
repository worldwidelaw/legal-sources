# MX/PoderJudicialBajaCaliforniaSur — Baja California Sur Court Decisions

Full-text court decisions (*sentencias en versión pública*) from the
**Tribunal Superior de Justicia del Estado de Baja California Sur**, Mexico.

## Source

- Portal: <https://e-tribunalbcs.mx/AccesoLibre/SentenciasPublicasBusqueda.aspx>
  ("Sentencias Públicas — Acceso Libre")
- Type: `case_law` (state judiciary, ISO 3166-2 **MX-BCS**)

## How it works

The portal is an ASP.NET WebForms search marked **Acceso Libre** (no auth):

1. `GET SentenciasPublicasBusqueda.aspx` to harvest `__VIEWSTATE` /
   `__EVENTVALIDATION`.
2. `POST` the search with the publication-year filter enabled (`txtAño=YYYY`)
   and all other selectors left at "VER TODOS" → the full result set for that
   year (all materias) renders in a single HTML table (~471 rows for 2026).
3. Each row exposes an encrypted document token via
   `AbrirSentenciaPublica('<token>')`; resolving `Documento.aspx?cadena=<token>`
   → `SentenciasPublicasPDF.aspx` streams the full-text PDF, whose text layer
   is extracted (~10K–200K chars/decision).

## Usage

```bash
python bootstrap.py test                    # connectivity + sample PDF check
python bootstrap.py bootstrap --sample      # save ~12 sample records
python bootstrap.py bootstrap-fast --full   # full run, streams to data/records.jsonl
```

## License

[Términos de Libre Uso de la Información (LIBRE USO MX)](https://datos.gob.mx/libreusomx) — Mexican government public information, freely reusable; commercial use permitted with attribution.

Public-version court decisions are published by the Baja California Sur state
judiciary as a transparency obligation (versiones públicas).
