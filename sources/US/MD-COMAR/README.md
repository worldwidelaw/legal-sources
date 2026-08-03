# US/MD-COMAR — Code of Maryland Regulations (COMAR)

The **Code of Maryland Regulations (COMAR)** is the official compilation of
the administrative regulations adopted by the agencies of the State of
Maryland. It is published by the **Division of State Documents** in the
Office of the Secretary of State and mirrored, as the free public *Library
of Maryland Regulations*, at [regs.maryland.gov](https://regs.maryland.gov/us/md/exec/comar).

Each COMAR regulation is an administrative rule with the force of law →
**legislation**. This source is distinct from `US/MD-Legislation`, which
covers the Maryland **Code / statutes** (`mgaleg.maryland.gov`); COMAR is the
**regulations**.

## Access

`regs.maryland.gov` exposes the whole code with no JavaScript, CAPTCHA or auth:

- **Tree:** `https://regs.maryland.gov/us/md/exec/comar/index.json` — a nested
  Title → Subtitle → Chapter → Section container.
- **Full text:** every **subtitle** node carries an `fh` field pointing at its
  `index.full.html`, e.g.
  `https://regs.maryland.gov/us/md/exec/comar/19A.02/index.full.html`.
  The `<article class="content">` body holds every chapter and regulation of
  that subtitle plus its **Administrative History** (adoption / amendment
  effective dates). Documents are born-digital (real text layer, **no OCR**).

There are ~1,850 subtitle documents across all COMAR titles. The subtitle is
the site's own canonical full-text unit (chapter-level `index.full.html`
files return 404).

## Fields

`_id`, `comar_code` (e.g. `19A.02`), `title`, `text` (full subtitle body),
`date` (earliest effective date), `url`, `jurisdiction` (`US-MD`).

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample subtitles
python bootstrap.py bootstrap           # full pull (all COMAR subtitles)
```

## License

[Public Domain — State of Maryland Government Work (edicts of government), 17 U.S.C. § 105 (analogous)](https://www.law.cornell.edu/uscode/text/17/105) — the Code of Maryland Regulations is an official public compilation of the State of Maryland's administrative regulations, published for public use with no copyright restriction. Commercial use permitted.
