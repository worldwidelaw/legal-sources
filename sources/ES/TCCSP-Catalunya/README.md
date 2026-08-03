# ES/TCCSP-Catalunya — Tribunal Català de Contractes del Sector Públic

Catalonia's regional public-procurement appeals tribunal (Spain). It resolves the
*recurs especial en matèria de contractació* and issues binding **resolucions**
(full text, in Catalan). Sibling of the central ES/TACRC (captcha-blocked) and the
built regional tribunals ES/TARCJA-Andalucia, ES/OARC-Euskadi, ES/TACP-Madrid,
ES/TACPA-Aragon, ES/TACPN-Navarra, ES/TACPC-Canarias and ES/TARCCyL.

## Data

- **Type:** case_law
- **Coverage:** 2012–present, ~2,000 resolucions
- **Language:** Catalan (ca)
- **Jurisdiction:** ES-CT (Catalunya)

## Access

No authentication. The resolucions are published on the Generalitat de Catalunya
contracting portal, organised by year:

```
https://contractacio.gencat.cat/ca/contacte/tccsp/resolucions-tccsp/{YEAR}/
```

Each year page lists 50 resolucions and paginates via a
`?page{TOKEN}={N}&googleoff=1` query — the `TOKEN` is the portlet instance id,
discovered from the page's own pagination links. Every listed resolució links
**directly** to a born-digital PDF under
`/web/.content/contacte/tccsp/resolucions/{YEAR}/resolucio_num._{N}_{YEAR}.pdf`.
Full text is extracted with PyMuPDF (fitz) — no OCR required. The número/year come
from the PDF filename; the decision date is parsed from the PDF header
(*"Barcelona, <day> de <month> de <year>"*).

The Socrata dataset `dkrd-id95` (Transparència Catalunya) is metadata-only and
stale to 2018, so it is **not** used.

## Usage

```bash
python bootstrap.py test                # connectivity + one-record smoke test
python bootstrap.py bootstrap --sample  # 12+ validation samples
python bootstrap.py bootstrap-fast      # full corpus (VPS entrypoint)
python bootstrap.py update              # incremental (current + previous year)
```

## License

[Generalitat de Catalunya open data (PSI reuse)](https://web.gencat.cat/ca/menu-ajuda/ajuda/avis_legal/) — reutilización de información del sector público (Ley 37/2007); attribution required, commercial use permitted.
