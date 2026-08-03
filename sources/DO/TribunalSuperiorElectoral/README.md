# DO/TribunalSuperiorElectoral

Full-text **contentious sentencias** of the Dominican Republic's **Tribunal
Superior Electoral (TSE)** — the country's highest electoral court, separate
from the Suprema Corte de Justicia and the Tribunal Constitucional.

## Source

- Listing: https://tse.do/category/decisiones/sentenciascontenciosas/
- Viewer / data: https://visorpdf.tse.do/

The TSE publishes its contentious decisions as machine-readable PDFs through a
standalone viewer. The viewer lists sentencias by year (2012–present),
paginated by `pos`:

```
https://visorpdf.tse.do/?pos={n}&y={year}&s=
```

Each list row links to a detail page that carries the sentencia number,
expediente, síntesis and publication date, plus an `<iframe>` to the PDF:

```
https://visorpdf.tse.do/documento/contenciosas/{token}
https://visorpdf.tse.do/file-upload/{ts}.pdf
```

PDFs are text-based (not scanned); full text is extracted with the shared
`common.pdf_extract` backend.

> **Note:** `tse.do` and `visorpdf.tse.do` return HTTP 403 to non-browser
> User-Agents. A desktop browser UA is required (the scraper sets one).

## Usage

```bash
python bootstrap.py test                  # connectivity / parse check
python bootstrap.py bootstrap --sample    # 12+ sample records
python bootstrap.py bootstrap-fast --full # full pull -> data/records.jsonl
```

## License

[Open Government Data](https://tse.do/) — public judicial decisions of a
Dominican Republic state court, published openly with no authentication.
Commercial use permitted.
