# ES/TARCJA-Andalucia — Tribunal Administrativo de Recursos Contractuales de la Junta de Andalucía

Resoluciones of the **Tribunal Administrativo de Recursos Contractuales de la
Junta de Andalucía (TARCJA)** — the Andalusian administrative tribunal that
resolves the *recurso especial en materia de contratación* (special appeals in
public procurement). It is the regional counterpart of the central tribunal
(ES/TACRC, which is captcha-blocked), and its data is fully open.

- **Country:** ES
- **Jurisdiction:** ES-AN (Andalucía)
- **Type:** case_law
- **Coverage:** ~6,600 resoluciones, 2011–present
- **Language:** Spanish (es)

## Data source

Official Junta de Andalucía open-data REST API. The entire dataset is returned
as a single JSON dump; the full text of each resolution is in a born-digital PDF.

- Dataset dump: `GET https://datos.juntadeandalucia.es/api/v0/tender-court-decisions/all?format=json`
- Count: `GET https://datos.juntadeandalucia.es/api/v0/tender-court-decisions/count`
- PDF: `https://www.juntadeandalucia.es{attached_file…uri}` (extracted with PyMuPDF, no OCR)

## Usage

```bash
python bootstrap.py test                  # connectivity + one full-text extract
python bootstrap.py bootstrap --sample    # 12 sample records
python bootstrap.py bootstrap             # full pull
python bootstrap.py bootstrap-fast        # concurrent full pull (VPS entrypoint)
python bootstrap.py update                # incremental (by resolution date)
```

## Output schema

Each record includes `_id`, `_source`, `_type`, `_fetched_at`, `title`, `text`
(full resolution body), `date`, `url`, plus `resolution_number`,
`resource_number`, `summary`, `type_contract`, `contested_act`,
`resolution_type`, `tribunal`, `jurisdiction`.

## License

[Junta de Andalucía open data — Aviso legal (reutilización PSI)](https://datos.juntadeandalucia.es/aviso-legal) — public-sector information reuse under Ley 37/2007 / RD 1495/2011. Commercial use permitted; attribution required.
