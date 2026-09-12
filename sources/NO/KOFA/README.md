# NO/KOFA — Norwegian Public Procurement Complaints Board

Klagenemnda for offentlige anskaffelser (KOFA) handles complaints about public procurement processes in Norway. ~4,900 decisions from 2003 to present.

## Data Access

- **API**: WordPress REST API at `/wp-json/wp/v2/sak` for case listing
- **HTML**: Case detail pages at `/sak/{slug}` for structured metadata
- **PDF**: Decision documents for full text (pypdf, falling back to `common/pdf_extract`
  for the pre-2011 decisions that are image-only scans)

## Cases without a decision document

Roughly 6–12% of the case list is closed without KOFA publishing anything — the
complaint was withdrawn (`Avgjørelse: Trukket`) or the secretariat dismissed it as
clearly unfounded or unsuited to written procedure (`Avvist - ...`). Those detail
pages carry an empty `<span class="pdflink"></span>` and an empty `Sammendrag`, so
there is no text to fetch. The scraper skips them instead of emitting textless
records, and raises if more than 35% of cases come back textless — that ratio would
mean the page layout changed rather than that the cases are genuinely empty.

Do **not** guess PDF URLs from the case slug: the document filenames do not track
case numbers (`documents/200538.pdf` is the decision in case *2006/38*, not 2005/38).

## Dates

`date` is the case's own closing date (`Avsluttet`), falling back to the registration
date and only then to the WordPress publish date. The detail tables mix `dd.mm.yyyy`
and bare `yyyymmdd`; pre-2018 cases were bulk-imported and all share a 2018 publish
date, so the WP date is a poor last resort.

## License

[NLOD 2.0](https://data.norge.no/nlod/en/2.0) — Norwegian License for Open Government Data. Attribution required.
