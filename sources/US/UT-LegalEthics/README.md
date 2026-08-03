# US/UT-LegalEthics — Utah State Bar Ethics Advisory Opinion Committee

Full text of the ethics advisory opinions issued by the **Ethics Advisory
Opinion Committee ("EAOC")** of the Utah State Bar. The EAOC issues formal
written opinions on the ethical propriety of the professional or personal
conduct of Bar members under the **Utah Rules of Professional Conduct** =
**doctrine** (the Bar's official written interpretation of the attorney-conduct
rules).

- **Publisher:** [Utah State Bar](https://www.utahbar.org/ethics-opinions/) (the integrated bar that regulates Utah lawyers)
- **Coverage:** ~286 opinions, 1970s to present (Opinion Nos. like `90-100`,
  `97-11`, `2001-05`, `22-04`, `25-01`)
- **Format:** born-digital PDF (older) + Elementor HTML (most recent) — no OCR,
  no CAPTCHA, no auth
- **Type:** doctrine

## How it works

1. **Enumerate** every opinion via the public WordPress REST API:
   `GET /wp-json/wp/v2/ethics-opinions?per_page=100&page=<n>` (paginated by the
   `X-WP-TotalPages` header). Each record carries `slug`, `title`, `link`,
   `date`. The REST `content` field is empty — the body lives in a PDF or the
   Elementor detail page.
2. **Resolve the PDF** via the media library:
   `GET /wp-json/wp/v2/media?search=<slug>`, picking the `.pdf` `source_url`
   whose filename stem matches the slug (2-digit-year slugs map to
   4-digit-year filenames, e.g. `22-04` → `2022-04.pdf`).
3. **Extract** the PDF text layer with PyMuPDF (no OCR). For the handful of very
   recent HTML-only opinions with no PDF, fall back to the Elementor detail
   page's `div.elementor-widget-theme-post-content`.

The date comes from the `Issued <Month Day, Year>` line in the opinion body,
otherwise the year encoded in the slug.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull (all opinions)
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

Requires **PyMuPDF** (`fitz`) for PDF text extraction.

## Distinct from

- **US/UT-JudicialEthics** — the Utah Judicial Ethics Advisory Committee, which
  advises *judges* on the Code of Judicial Conduct.
- **Utah Attorney General opinions** — legal opinions of the state AG.

This source covers *attorney* professional-responsibility opinions, part of the
state-bar legal/attorney-ethics vein (alongside `US/NC-LegalEthics`,
`US/AZ-LegalEthics`, `US/TX-LegalEthics`).

## License

[Public Domain — U.S. Government / State Regulatory-Agency Official Record](https://www.law.cornell.edu/uscode/text/17/105) — ethics advisory opinions of the Utah State Bar's Ethics Advisory Opinion Committee are official public records of the Utah State Bar (the integrated bar that regulates lawyers in Utah), published on the State Bar website for public use with no copyright restriction. Commercial use permitted; no attribution required.
