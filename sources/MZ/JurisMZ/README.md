# MZ/JurisMZ — Mozambique Case Law Database (JURIS MZ)

**"Jurisprudência Moçambicana"** — the official ECLI-indexed case-law database of the
Supreme Court of Mozambique (**Tribunal Supremo**), hosted at
[juris.ts.gov.mz](https://juris.ts.gov.mz) and produced with the technical support of
the Portuguese Superior Council of the Judiciary (CSM) under the EU-funded *Projeto
Íntegra*.

## What it collects

Full-text acórdãos (court decisions) of the Tribunal Supremo, each addressed by an
**ECLI** (e.g. `JURIS:MZ:TS:2023:2`). Unlike the raw WordPress-media PDFs behind
`MZ/CourtSupremo`, JURIS publishes decisions as **clean HTML** with structured sections:

- **Sumário** — the headnote / reasoning summary
- **Decisão Texto Parcial** — the full reasoning of the decision
- **Decisão Texto Integral** — the operative ruling
- Metadata: relator, processo, data do acórdão, área temática

The `text` field concatenates Sumário + Decisão Texto Parcial + Decisão Texto Integral.
No OCR or PDF extraction is required.

## Access

- **List:** `GET /items/loadItems?perPage=100&page=1` (send header
  `X-Requested-With: XMLHttpRequest`) → JSON `{records: [...], totalRecordCount}`.
- **Detail:** `GET /juris/{ecli}/` → HTML record page.

Corpus is a curated beta (~94 acórdãos as of 2026). Portuguese language.

## Usage

```bash
python bootstrap.py test                # connectivity check
python bootstrap.py bootstrap --sample  # 15 sample records
python bootstrap.py bootstrap-fast      # full run (fleet entry point)
```

## License

[Public Domain (Government)](https://juris.ts.gov.mz/advertencia) — official court
decisions of Mozambique are public domain. Database produced by the Tribunal Supremo
with CSM Portugal / EU *Projeto Íntegra* support. Commercial use permitted.
