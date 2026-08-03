# ES/TARCCyL — Tribunal Administrativo de Recursos Contractuales de Castilla y León

Regional public-procurement appeals tribunal for Castilla y León (Spain), hosted
by the Consejo Consultivo de Castilla y León (cccyl.es). It resolves the *recurso
especial en materia de contratación pública* and issues binding **resoluciones**
(full text). Sibling of the central ES/TACRC (captcha-blocked) and the built
regional tribunals ES/TARCJA-Andalucia, ES/OARC-Euskadi, ES/TACP-Madrid,
ES/TACPA-Aragon, ES/TACPN-Navarra and ES/TACPC-Canarias.

## Data

- **Type:** case_law
- **Coverage:** ~2017–present, ~600–1,000 resoluciones
- **Language:** Spanish (es)
- **Jurisdiction:** ES-CL (Castilla y León)

## Access

No authentication. The resoluciones section is organised by year:

```
https://www.cccyl.es/es/tribunal-administrativo-recursos-contractuales-castilla-leo/resoluciones/resoluciones-ano-{YEAR}
```

Each year page lists ~10 resoluciones and paginates via a `.nodos,{offset},10`
suffix (offset 0, 10, 20, …). Every listed resolución links **directly** to a
born-digital PDF under its `.ficheros/{id}-Resolucion...pdf` path. Full text is
extracted with PyMuPDF (fitz) — no OCR required. The resolución number/year come
from the URL path; the decision date is parsed from the PDF header
(*"Resolución N/YYYY, de <day> de <month>…"*).

## Usage

```bash
python bootstrap.py test                # connectivity + one-record smoke test
python bootstrap.py bootstrap --sample  # 12+ validation samples
python bootstrap.py bootstrap-fast      # full corpus (VPS entrypoint)
python bootstrap.py update              # incremental (current + previous year)
```

## License

[Junta de Castilla y León open data (PSI reuse)](https://datosabiertos.jcyl.es/web/es/aviso-legal.html) — reutilización de información del sector público (Ley 37/2007); attribution required, commercial use permitted.
