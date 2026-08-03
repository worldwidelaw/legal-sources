# ES/TACPC-Canarias — Tribunal Administrativo de Contratos Públicos de la Comunidad Autónoma de Canarias

Regional public-procurement appeals tribunal for the Canary Islands (created by
Decreto 10/2015). It resolves the *recurso especial en materia de contratación*
(special appeals in public procurement) and issues binding *resoluciones*.

Sibling of the built **ES/TARCJA-Andalucia**, **ES/OARC-Euskadi**,
**ES/TACP-Madrid** and **ES/TACPA-Aragon**, and of the captcha-blocked central
tribunal **ES/TACRC**.

## Data access

The tribunal publishes its resoluciones through a Solr-backed search *visor*
(OpenCms portal). Listing is a server-rendered HTML `POST`:

```
POST https://www.gobiernodecanarias.org/hacienda/contratacion/tacp/visor_resoluciones/index.jsp
  param_accion=buscar
  chpae_resolucion_fecharesolucion_es_dt_d=DD/MM/YYYY   (fecha desde)
  chpae_resolucion_fecharesolucion_es_dt=DD/MM/YYYY      (fecha hasta)
```

Each result `<li>` carries the resolution number (`núm/año`), fecha de
resolución, expediente, resumen and a link to a **born-digital PDF** at
`/cmsgob1/export/sites/.../RES-*.pdf` (text layer, no OCR). Full text is
extracted with PyMuPDF (`fitz`).

### Gotchas

- The visor **caps the rendered result set at 200 items** regardless of the
  reported "Se han encontrado N resultado/s" total, and server-side `param_page`
  pagination is a no-op. The scraper windows the search **by year**, subdividing
  into **months** whenever a year exceeds the 200-item cap.
- **Full-text PDFs are only published from ~2020 onward** (2022–present are
  complete; the tribunal is "paulatinamente" back-filling older years). Earlier
  years (2015–2019) appear in the index as metadata-only rows with **no PDF
  link** — these are correctly skipped (full text is mandatory).
- Full-field search only works for 2023–present; the **fecha desde/hasta** filter
  used here works for all years, so it is the reliable windowing key.

## Coverage

- ~1,146 full-text resoluciones, 2020–present (bulk 2022–present).
- Type: `case_law` (jurisdiction `ES-CN`).
- Language: Spanish (`es`).

## Usage

```bash
python bootstrap.py test                 # connectivity test
python bootstrap.py bootstrap --sample   # 12+ validation samples
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # concurrent full pull (VPS entrypoint)
python bootstrap.py update               # incremental update
```

## License

[Gobierno de Canarias open data (PSI reuse)](https://datos.canarias.es/portal/aviso-legal) — reutilización de información del sector público (Ley 37/2007). Commercial use permitted; attribution required.
