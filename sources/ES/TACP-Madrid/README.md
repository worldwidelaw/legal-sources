# ES/TACP-Madrid — Tribunal Administrativo de Contratación Pública de la Comunidad de Madrid

Regional public-procurement appeals tribunal for the Community of Madrid
(**ES-MD**). It resolves the *recurso especial en materia de contratación*
(special appeals in public procurement) and issues binding **resoluciones**.

This is the **largest** of the Spanish regional TARC set and a sibling of:
- `ES/TARCJA-Andalucia` — the Andalusian counterpart (built, JSON API)
- `ES/OARC-Euskadi` — the Basque counterpart (built)
- `ES/TACPN-Navarra` — the Navarre counterpart (planned)
- `ES/TACRC` — the **central** state tribunal (captcha-blocked)

## Data

- **~5,791 resoluciones**, roughly 2011–present
- Type: `case_law`
- Language: Spanish (`es`)
- Full text: **yes** — born-digital PDFs with a text layer (no OCR)

## Access

The public search UI at
<https://www.comunidad.madrid/tacp/busquedaresoluciones> is a Drupal *Views*
listing. Its result rows already carry the resolution number, date, thematic
summary, and a link to the born-digital PDF, so no per-resolution detail-page
fetch is required.

Pagination is driven by the Drupal Views AJAX endpoint:

```
POST https://www.comunidad.madrid/tacp/views/ajax
     view_name=busquedaresoluciones&view_display_id=page&page=N&_drupal_ajax=1
     (header: X-Requested-With: XMLHttpRequest, Referer: the search URL)
```

which returns a JSON array of AJAX commands; the `insert` command holds the
rendered results HTML (5 resoluciones per page → ~1,159 pages). Each resolution's
full text is fetched from its PDF under
`https://www.comunidad.madrid/tacp/sites/default/files/{filename}` (or the
tokenised `/tacp/file/{fid}/download` href in the row) and extracted with
PyMuPDF (`fitz`).

**Note:** the listing anchor text for the resolution number is occasionally
mistyped upstream (e.g. `251/22026`), so the canonical resolution and expediente
numbers are parsed from the authoritative PDF filename
(`resolucion_251-2026_expediente_197-2026.pdf`).

**Vantage:** the host serves this build vantage fine with a browser User-Agent +
`Referer` header. Earlier probes reported a 403 for a plain UA; from a datacenter
IP the fleet may need a residential/EU vantage.

## Usage

```bash
python bootstrap.py test                  # connectivity + one full-text extraction
python bootstrap.py bootstrap --sample    # 12+ sample records for validation
python bootstrap.py bootstrap             # full initial pull
python bootstrap.py bootstrap-fast        # concurrent full pull (VPS entrypoint)
python bootstrap.py update                # incremental (newest-first, stops at cutoff)
```

## License

[Comunidad de Madrid — Aviso legal / reutilización de información del sector público](https://www.comunidad.madrid/gobierno/informacion-institucional/aviso-legal) — public-sector information reuse under Ley 37/2007 (Spanish PSI law). These are public resolutions of an administrative tribunal. Commercial use permitted; attribution required.
