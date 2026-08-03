# INTL/CentralAmericanCourt — Corte Centroamericana de Justicia (CCJ / SICA)

Full text of the **Gaceta Oficial** (official jurisprudence gazette, Nos 1–19)
of the **Central American Court of Justice** (Corte Centroamericana de
Justicia, CCJ), the judicial organ of the Central American Integration System
(SICA), seated in Managua, Nicaragua since 1994.

Each Gaceta Oficial issue reproduces the Court's **sentencias** (definitive
judgments), **resoluciones** and interlocutory awards interpreting and applying
the SICA treaties/protocols. These decisions are binding and non-appealable on
member states, SICA organs and natural/legal persons — i.e. `case_law`.

## Access

- **Live host** `https://portal.ccj.org.ni/` (WordPress) TCP-times-out on :443
  from foreign/datacenter vantages (Nicaragua-hosted, geo/IP filtered).
- The **Internet Archive** holds full 200-status captures of the entire Gaceta
  Oficial series under `portal.ccj.org.ni/ccj/wp-content/uploads/Gaceta*.pdf`.
- The scraper enumerates them via the **Wayback CDX API**, downloads each via
  the raw `id_` endpoint, and **OCRs** all pages (scanned image PDFs) with
  tesseract (`lang=spa`, 200 dpi). OCR output is clean, continuous Spanish
  legal prose.

One full-text record is emitted per Gaceta Oficial issue (19 issues). Reliable
per-sentence splitting from OCR text is not attempted; each issue is a coherent
official publication of the Court's decisions for its period.

## Requirements

`PyMuPDF` (fitz), `pytesseract` + the `tesseract` binary with the Spanish
language pack (`spa`), and `Pillow`.

## Usage

```bash
python bootstrap.py test-api             # enumerate + OCR-check first Gaceta
python bootstrap.py bootstrap --sample   # ~12 samples
python bootstrap.py bootstrap            # full pull (all 19 Gacetas)
```

## License

[Public Domain — government edicts / international tribunal](https://www.law.cornell.edu/uscode/text/17/105) — official decisions and gazettes of an international tribunal (organ of SICA) are government-edict works, not subject to copyright. Commercial use permitted; no attribution required.
