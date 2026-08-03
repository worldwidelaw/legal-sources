# MX/PoderJudicialNayarit — Nayarit State Court Decisions

Full-text court decisions (*sentencias dictadas*, versiones públicas) from the
**Poder Judicial del Estado de Nayarit**, Mexico.

## Source

- Portal: <https://tribunalvirtual-tsjnay.gob.mx/publico/sentencias.public.php>
  ("Tribunal Virtual — Sentencias Dictadas")
- Type: `case_law` (state judiciary, ISO 3166-2 **MX-NAY**)

## How it works

The public search form posts to `app/sentencias.buscar.public.php` with a static
`Authorization` header. Submitting only the `anio` (year) field returns **all**
decisions for that year as JSON:

```json
{"resultados": [{"expediente", "juzgado", "materia",
                 "fecha_resolucion", "tipo_registro", "archivo"}, ...]}
```

The `archivo` field is a relative path to the full-text PDF
(`documentos/YYYY/<file>.pdf`), resolved against the `/publico/` base. The scraper
iterates year by year (2008..present), downloads each PDF, and extracts its text.

## Usage

```bash
python bootstrap.py test                 # connectivity + sample PDF check
python bootstrap.py bootstrap --sample   # save ~12 sample records
python bootstrap.py bootstrap-fast --full  # full run, streams to data/records.jsonl
```

## License

[Términos de Libre Uso de la Información (LIBRE USO MX)](https://datos.gob.mx/libreusomx) — Mexican government public information, freely reusable; commercial use permitted with attribution.

Public-version court decisions are published by the Nayarit state judiciary as a
transparency obligation (versiones públicas).
