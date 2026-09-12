# CO/DIAN-TaxDoctrine — Colombian Tax Authority Doctrine

Conceptos y oficios tributarios issued by Colombia's **Dirección de Impuestos y
Aduanas Nacionales (DIAN)**, published in the official Normograma legal
compilation maintained for DIAN by Avance Jurídico Casa Editorial.

- **Portal:** https://normograma.dian.gov.co/dian/compilacion/t_2_doctrina_tributaria.html
- **Type:** `doctrine`
- **Language:** Spanish
- **Corpus:** 15,960 documents (index-verified 2026-08-04), 1987 to present
- **Coverage:** income tax, VAT, withholding, transfer pricing, customs duties,
  exchange controls; includes unified concepts, general concepts, compilatory
  oficios, and Consejo de Estado Sala de Consulta concepts adopted by DIAN.

## How it works

Discovery is **index-driven**. The tree page `t_2_doctrina_tributaria.html`
renders empty branch panels and lazily loads their contents over XHR from
sibling files:

```
t_2_doctrina_tributaria_parte_01.html   … _parte_13.html   (14 → HTTP 404)
```

Those parts carry an `<a href="docs/....htm">` for every document in the
compilation. `fetch_all()` walks the parts until a 404, caches the resulting
list in `data/doc_index.json` (7-day TTL), then fetches each document page and
extracts full text from its `.panel-documento` div. Pages are served as
ISO-8859-1.

**Do not brute-force the number space.** Slugs are irregular — six-digit numbers
(`oficio_dian_915014_2022`), letter suffixes (`oficio_dian_8937a_2025`), the
`concepto_tributario_dian_*` family and Consejo de Estado concepts
(`CE-SC-RAD2005-N1650`) are all in the corpus and none are reachable by a
sequential `1..20000` sweep.

Long runs are resumable: completed document paths are checkpointed to
`data/fetch_checkpoint.json` every 100 fetches, so a restart skips them with no
network calls. Every request carries a `(connect, read)` timeout so a slow-drip
response cannot wedge the run.

## Usage

```bash
python bootstrap.py test                 # index part 01 + one known document
python bootstrap.py bootstrap --sample   # 15 sample records → sample/
python bootstrap.py bootstrap            # full corpus → data/records.jsonl
python bootstrap.py bootstrap-fast       # alias used by the fleet wrapper
python bootstrap.py update               # documents from the last two years
```

At ~0.6 s/request a full crawl of 15,960 documents takes roughly 3 hours.

## License

[Colombia Open Government Data — Ley 1712 de 2014 (Ley de Transparencia y del
Derecho de Acceso a la Información Pública)](https://normograma.dian.gov.co/dian/compilacion/t_2_doctrina_tributaria.html)
— DIAN doctrine is official public information of a Colombian state entity and
is freely accessible without registration. Commercial use permitted; attribution
to DIAN is expected.
