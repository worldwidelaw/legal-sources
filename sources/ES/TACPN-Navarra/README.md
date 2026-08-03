# ES/TACPN-Navarra — Tribunal Administrativo de Contratos Públicos de Navarra

Regional public-procurement appeals tribunal for the Chartered Community of
Navarra (Comunidad Foral de Navarra). It resolves the *reclamación especial en
materia de contratación pública* (special appeals in public procurement) and
issues binding **acuerdos**. This is the open-data-accessible regional
counterpart to the captcha-blocked central tribunal (`ES/TACRC`), and a sibling
of `ES/TARCJA-Andalucia` and `ES/OARC-Euskadi`.

## Data source

- **Portal:** https://portalcontratacion.navarra.es/es/todos-acuerdos
- **Access:** Public Liferay Asset Publisher list (20 acuerdos/page) linking
  directly to **born-digital PDFs** under `/documents/...`. Paginate the
  `_cur` param until a page returns no PDF links (~55 pages, ~1,095 acuerdos,
  ~2010–present).
- **Full text:** extracted from each born-digital PDF with PyMuPDF (`fitz`),
  no OCR needed. Acuerdo number and date are parsed from the PDF body
  (`ACUERDO N/YYYY, de <día> de <mes> ...`).
- **Type:** `case_law` — Spanish (`es`).
- **Auth:** none (open data).

## Usage

```bash
python bootstrap.py test                 # connectivity + one-record check
python bootstrap.py bootstrap --sample   # 12+ validation samples
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # full pull (VPS entrypoint)
python bootstrap.py update               # incremental (newest pages)
```

## License

[Gobierno de Navarra open data (PSI reuse)](https://gobiernoabierto.navarra.es/es/open-data/informacion-practica/aviso-legal) — reutilización de información del sector público (Ley 37/2007); attribution required, commercial use permitted.
