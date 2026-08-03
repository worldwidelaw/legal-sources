# MX/PoderJudicialHidalgo — Poder Judicial del Estado de Hidalgo

Full-text court decisions (*sentencias en versión pública*) from the **Tribunal
Superior de Justicia del Estado de Hidalgo**, Mexico, via the public
**Observatorio de Sentencias** portal.

- **Portal:** https://www.pjhidalgo.gob.mx/transparencia/observatorio/sentencias.html
- **Type:** `case_law`
- **Jurisdiction:** MX-HID (Hidalgo)
- **Auth:** none

## How it works

The Observatorio listing page (`sentencias.html`) groups the published
sentencias into tabbed *materias* — Civil, Familiar, Mercantil, Penal (Sistema
Acusatorio) and Penal (Sistema Tradicional). Each entry links directly to a
full-text PDF under `sentencias/<MATERIA>/<file>.pdf`. The scraper:

1. `GET sentencias.html` once and collects every `sentencias/.../*.pdf` link.
2. Downloads each PDF and extracts its text layer.

### PDF text extraction note

These court PDFs lay out each character individually but embed real space
glyphs. pdfplumber's gap-based `extract_text()` therefore inserts spurious extra
spaces (`"P a ch u ca"`). The scraper instead reads the raw char stream per
visual line and concatenates it, reproducing the document's own spacing
faithfully (`"Pachuca de Soto"`). Scanned PDFs (rare here) fall back to the
shared `common.pdf_extract` backend (opendataloader → pdfplumber → OCR).

## Coverage

This is the **public Observatorio subset** (~49 sentencias across the five
materias, full legal reasoning). The complete sentencia-publication system is
staff-login-gated, so a curated public subset is expected.

## Usage

```bash
python bootstrap.py test
python bootstrap.py bootstrap --sample
python bootstrap.py bootstrap-fast --full
```

## License

[LIBRE USO MX — Términos de Libre Uso de la Información (datos.gob.mx)](https://datos.gob.mx/libreusomx) — Mexican government public data; commercial use permitted with attribution.

Public-version court decisions are published by the Hidalgo state judiciary as a
transparency obligation (*versiones públicas*) and are freely reusable.
