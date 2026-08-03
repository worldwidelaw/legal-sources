# ES/TACPA-Aragon — Tribunal Administrativo de Contratos Públicos de Aragón

Regional public-procurement appeals tribunal for Aragón (**ES-AR**). It resolves
the *recurso especial en materia de contratación* and *cuestiones de nulidad* in
public procurement and issues binding **acuerdos**.

Sibling of:
- `ES/TARCJA-Andalucia` — Andalusian counterpart (built, JSON API)
- `ES/OARC-Euskadi` — Basque counterpart (built)
- `ES/TACP-Madrid` — Madrid counterpart (built, largest)
- `ES/TACRC` — the **central** state tribunal (captcha-blocked)

## Data

- **~1,902 acuerdos**, 2011–present
- Type: `case_law`
- Language: Spanish (`es`)
- Full text: **yes** — born-digital PDFs with a text layer (no OCR)

## Access

The tribunal publishes through a **BASIS/BRSCGI** full-text search engine at
`gd.aragon.es`. The listing command returns rows of inline metadata (número,
resumen temático, tipo de contrato, sentido del acuerdo, fecha) plus a link to
each acuerdo's document:

```
GET https://gd.aragon.es/cgi-bin/ACTA/BRSCGI
    ?CMD=VERLST&BASE=ACTA&DOCS=1-100&SEC=ACTA_PUBL&SORT=@FEPU
    &SEPARADOR=&TEMA-C=&NUME-C=&TIPO-C=&SENA-C=
    &@FERE-GE=20110101&@FERE-LE=20261231
→ "Documentos 1 a 100 de 1902"; paginate with DOCS=101-200, 201-300, ...
```

**GOTCHA:** BRSCGI rejects a query that has no indexed field term
(*"Introduzca un término de búsqueda"*) — **unless the full set of (empty) form
fields** (`SEPARADOR`, `TEMA-C`, `NUME-C`, `TIPO-C`, `SENA-C`) is supplied
alongside the `@FERE` date range. That combination is what turns the date range
itself into the query. A partial query (date range only) is rejected.

Each row's full text is the born-digital PDF served by
`.../BRSCGI?CMD=VEROBJ&MLKOB={id}`, extracted with PyMuPDF (`fitz`). No OCR.

The listing "Fecha de adopción del acuerdo" is present for ~70% of rows; for the
rest the acuerdo date is parsed from the PDF body (Spanish long-form date).

Despite the earlier recipe note, the documents are **PDF, not HTML**.

## Usage

```bash
python bootstrap.py test                  # connectivity + one full-text extraction
python bootstrap.py bootstrap --sample    # 12+ sample records for validation
python bootstrap.py bootstrap             # full initial pull
python bootstrap.py bootstrap-fast        # concurrent full pull (VPS entrypoint)
python bootstrap.py update                # incremental
```

## License

[Gobierno de Aragón — Aviso legal / reutilización de información del sector público](https://opendata.aragon.es/aviso-legal) — public-sector information reuse under Ley 37/2007 (Spanish PSI law). These are public acuerdos of an administrative tribunal. Commercial use permitted; attribution required.
