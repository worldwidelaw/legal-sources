# MX/PoderJudicialQuintanaRoo — Poder Judicial del Estado de Quintana Roo

Public-version court decisions (sentencias públicas) from the **Tribunal
Superior de Justicia del Estado de Quintana Roo**, accessed through the
public *Módulo de Consulta Ciudadana de Sentencias Públicas*.

- **Portal:** https://gestionchetumal.tsjqroo.gob.mx/sentenciaspublicas
- **Coverage:** sentences from the juzgados and salas across Quintana Roo's
  judicial districts (Chetumal, Cancún, Playa del Carmen, etc.) in civil,
  familiar, mercantil and penal matters.
- **Jurisdiction:** MX-ROO (state of Quintana Roo)
- **Type:** `case_law`

## How it works

The portal landing page lists every court as a link to
`Home/Consultas?id={N}` (N = 1..92). Each Consultas page server-renders a
table (`#tablaJuzgado`) of that court's sentences with columns: expediente,
juzgado, materia, juicio, tema, publicación date, and an `onclick`
`download('//<host>/<path>/X.pdf')` handler.

The PDFs live on an internal file host reachable only through the portal's
own proxy endpoint `Home/GetPdf?FilePath=<dir>&fileName=<file.pdf>`. The
scraper translates each onclick path into that proxy URL, downloads the PDF,
and extracts its full text via `common.pdf_extract`.

A few courts (ids 17, 90, 91) load their rows from separate JSON APIs
(`gestionjudicial` / `lexcifam`) instead of server-rendering them; those are
also handled.

## Usage

```bash
python bootstrap.py test                 # connectivity + one-PDF extraction check
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap-fast --full
```

## License

[Open government data](https://datos.gob.mx) — public-version judicial
decisions published by a Mexican state court for transparency. Commercial
use permitted; no attribution required.
