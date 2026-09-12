# AR/CABA-Legislation — Ciudad Autónoma de Buenos Aires Legislation (CEDOM)

**Source:** [https://www.buenosaires.gob.ar/gobierno/CEDOM](https://www.buenosaires.gob.ar/gobierno/CEDOM)
**Data types:** legislation

## Access

Norms are enumerated per (type, year) from the Boletín Oficial REST API at
`api-restboletinoficial.buenosaires.gob.ar`, and each norm's PDF is resolved from
its `link_documento_normas` entry, then `/getUrlDocument/{id}/{tipo}`, then
`link_anexo`. Text is extracted from that PDF.

The `ck_{archivo_norma}` URL this scraper originally guessed at is dead — it now
redirects to `noexiste.html` for every year — which is what issue #1466 reported.

## Coverage

| Period | Full text |
|---|---|
| 2018–present | Complete — every norm publishes its own document |
| 2017 | Partial — recovered via `link_anexo` (~5 of 10 sampled) |
| 2008–2016 | None — the API publishes no per-norm document, only the whole-day gazette PDF |
| before 2008 | Not indexed — the API returns no results |

Norms with no document are counted and reported at the end of a run, not emitted:
a metadata-only record is not acceptable for this project, and the whole-day
gazette is a different document from the norm.

## License

Official Buenos Aires City legislation — published via the Boletin Oficial REST API. Argentine legislation is public domain under Argentine law. Published under Buenos Aires open data policy ([BA Data](https://data.buenosaires.gob.ar)).
