# MX/PoderJudicialJalisco — Poder Judicial del Estado de Jalisco

**Source:** [https://publicacionsentencias.stjjalisco.gob.mx/](https://publicacionsentencias.stjjalisco.gob.mx/)
**API:** `https://publica-sentencias-backend.stjjalisco.gob.mx/`
**Data types:** case_law
**Jurisdiction:** MX-JAL (Supremo Tribunal de Justicia del Estado de Jalisco)

Full-text appellate judgments (*tocas*) published under the Jalisco transparency
mandate — civil, penal, familiar and administrative salas, 2014 to present.

## Access strategy

The paginated listing endpoint `/tocas` is gated behind reCAPTCHA v3 and returns
`403 {"error":"reCAPTCHA requerido"}`. The per-document endpoints are open:

| Endpoint | Returns | Rate limit |
|----------|---------|------------|
| `GET /toca/{id}` | JSON metadata (sala, materia, magistrado, fechas) | 30/min |
| `GET /toca/{id}/file` | the sentencia PDF (born-digital) | 20/min |

So the corpus is enumerated over the toca id space (1 … ~88,300) rather than
paginated. The id space is dense — nearly every id resolves — and the PDF is the
real judgment text (16K–400K chars), not an AI summary.

## Throughput and resuming

The PDF route caps the crawl at ~20 documents/minute, so a full sweep of ~88,300
tocas runs for tens of hours and will not finish inside a single fleet run.
It is designed to resume:

- `data/checkpoint.json` records the last id yielded (rewound by 250 on resume to
  cover in-flight ids).
- If that file is missing but `data/records.jsonl` survived, the resume point is
  derived from the highest `MX-JAL-{id}` already written.

**Keep `data/` between runs**, otherwise each run re-walks the same head of the
id space and the corpus never advances.

The ceiling is re-probed at run time with a gap-tolerant window scan, so newly
published tocas are picked up without editing `KNOWN_MAX_ID`.

## Usage

```bash
python bootstrap.py test               # connectivity + one full record
python bootstrap.py bootstrap --sample # 15 samples spread across the id space
python bootstrap.py bootstrap-fast     # full sweep -> data/records.jsonl
python bootstrap.py update             # newest ~2,000 ids only
```

`bootstrap-fast` prefers PyMuPDF (`fitz`) and falls back to `pypdf`/`PyPDF2`.

## License

[Open government data](https://datos.gob.mx) — sentencias published under the
LGTAIP transparency mandate. Personal data is already redacted at source
(`* * * *` in the judgment text).
