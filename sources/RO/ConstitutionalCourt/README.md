# RO/ConstitutionalCourt — Curtea Constituțională a României

## What is this source?

The Romanian Constitutional Court (Curtea Constituțională a României) publishes its decisions and rulings as PDFs on [ccr.ro](https://www.ccr.ro). This scraper collects case law including admission decisions, relevant decisions, and rulings.

## Data types

- `case_law` — Constitutional Court decisions (Decizii) and rulings (Hotărâri)

## How it works

The scraper fetches HTML listing pages from four sections:
- `/jurisprudenta/jurisprudenta-decizii-de-admitere/` — Admission decisions
- `/jurisprudenta/decizii-relevante/` — Relevant decisions
- `/jurisprudenta/hotarari-de-admitere/` — Admission rulings
- `/jurisprudenta/hotarari-relevante/` — Relevant rulings

PDF links are extracted from each page and text is extracted using `common/pdf_extract.extract_pdf_markdown`.

## Quirks and limitations

- No public API — HTML scraping only
- PDFs are hosted on WordPress (`wp-content/uploads`)
- SSL certificate verification must be disabled (`verify=False`)
- Site blocks default Python user agents — browser-like headers required
- Dates are approximated from the upload path (YYYY/MM)
- Language: Romanian
- Coverage: 1992–present
