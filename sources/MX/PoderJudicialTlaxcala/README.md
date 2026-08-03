# MX/PoderJudicialTlaxcala — Tlaxcala State Court Decisions

Full-text court decisions (*sentencias en versión pública*) from the
**Tribunal Superior de Justicia del Estado de Tlaxcala**, Mexico.

## Source

- Portal: <https://tsjtlaxcala.gob.mx/transparencia/sps/sps1.html>
  ("Sentencias — Publicación en versión pública")
- Listing: <https://tsjtlaxcala.gob.mx/transparencia/sps/tabla1.html>
- Type: `case_law` (state judiciary, ISO 3166-2 **MX-TLA**)

## How it works

The portal exposes a single static HTML table (no auth):

1. `GET tabla1.html` → one `<tr>` per decision, columns
   `[expediente, tipo de juicio, año, juzgado, <a href="sentencias/FILE.pdf">]`
   (~6,560 rows).
2. `GET sentencias/<file>.pdf` → the full-text PDF; its text layer is
   extracted (~12K–43K chars/decision).

## Usage

```bash
python bootstrap.py test                    # connectivity + sample PDF check
python bootstrap.py bootstrap --sample      # save ~12 sample records
python bootstrap.py bootstrap-fast --full   # full run, streams to data/records.jsonl
```

## License

[Términos de Libre Uso de la Información (LIBRE USO MX)](https://datos.gob.mx/libreusomx) — Mexican government public information, freely reusable; commercial use permitted with attribution.

Public-version court decisions are published by the Tlaxcala state judiciary as a
transparency obligation (versiones públicas).
