# ES/TACGal-Galicia — Tribunal Administrativo de Contratación Pública de Galicia

Galicia's regional public-procurement appeals tribunal (TACGal), attached to the
Xunta de Galicia. It resolves the *recurso especial en materia de contratación
pública* under Ley 9/2017 and issues binding **resolucións** — regional
procurement case law. TACGal has operated since 2 April 2018.

## Source

- **Portal:** https://tacgal.xunta.gal/sixtacweb/resolucions/?locale=es
- **Type:** case_law
- **Coverage:** ~1,992 resolucións, 2018–present
- **Language:** Galician / Spanish (gl/es)

## How it works

The public "Resolucións" search (the `sixtacweb` app) renders a paginated HTML
table (100 rows/page) at
`/sixtacweb/resolucions/?locale=es&page={N}&size=100`. Each row carries the
resolution number, recurso number, decision date, decision
(Estima/Desestima/...), contract type, appealed act and a short description, plus
a download anchor whose `id` is the document id.

Each document is downloaded from `/sixtacweb/resolucions/download/{doc_id}`,
which returns HTTP 200 `application/pdf`. The stored file is a **born-digital
PDF wrapped in a multipart/form-data envelope**; the inner PDF (from the first
`%PDF` marker to the last `%%EOF`) is extracted and read with PyMuPDF (fitz). No
OCR is needed. The download endpoint does **not** enforce the site's reCAPTCHA
(the JS attaches a token only for logging).

## Usage

```bash
python bootstrap.py test                # connectivity + one-record check
python bootstrap.py bootstrap --sample  # 12+ validation samples
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # full pull (VPS entrypoint)
python bootstrap.py update              # incremental (recent pages)
```

## Related sources

Regional sibling of the other Spanish autonomous-community procurement tribunals
already built: `ES/TARCJA-Andalucia`, `ES/OARC-Euskadi`, `ES/TACPN-Navarra`,
`ES/TACPA-Aragon`, `ES/TCCSP-Catalunya`, `ES/TARCCyL`, `ES/TACPC-Canarias`. It is
the open-data alternative to the captcha-blocked central tribunal `ES/TACRC`.

## License

[Xunta de Galicia — Aviso legal / reutilización (Ley 37/2007)](https://www.xunta.gal/aviso-legal) — reuse of public-sector information permitted, attribution required. Commercial use permitted.
