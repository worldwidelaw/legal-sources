# US/PA-EthicsOpinions — Pennsylvania State Ethics Commission: Opinions and Advices of Counsel

Full text of the **Opinions of the Commission** and **Advices of Counsel**
issued by the **Pennsylvania State Ethics Commission** interpreting the Public
Official and Employee Ethics Act (65 Pa.C.S. Ch. 11) and the Commission's
regulations (51 Pa. Code).

An Opinion / Advice is the Commission's written interpretation of the Ethics Act
applied to a requester's facts, requested by a public official, public employee,
candidate, nominee or their authorized representative. Both series are
**doctrine** (advisory interpretations of statute). Each is a public record of a
Pennsylvania state agency (65 Pa.C.S. § 1108(k)).

## Source

The Commission's public **Ethics eLibrary** is a Laserfiche WebLink 11 portal:

- Portal: <https://www.ethicsrulings.pa.gov/WebLink/Browse.aspx?dbid=0&repo=Ethics>
- Landing page (redirected from ethics.pa.gov): <https://www.pa.gov/agencies/ethics/ethics-search/ethics-elibrary>

Structure: `Rulings > Ethics > Opinions` (folder 36759) and
`Rulings > Ethics > Advices` (folder 34015). Each holds year folders from 1979
to the present; each year folder holds the individual rulings as imaged
documents with an OCR text layer.

## How it works

1. **Session** — `GET /Welcome.aspx?cr=1` then `GET /Browse.aspx` to obtain the
   `WebLinkSession` + `AcceptsCookies` cookies (the API returns HTTP 500 /
   "Cookies are not enabled" without them).
2. **Enumerate** — `POST FolderListingService.aspx/GetFolderListingIds`
   returns all child entry ids uncapped (`GetFolderListing2` caps at 40 and
   ignores `start`/`end`). Walk each root → year folders → document ids, using
   `GetFolderListing2` (ascending + descending, deduped) to map each id to its
   name + page count, falling back to `GetDocumentInfo` for the page count.
3. **Full text** — `POST DocumentService.aspx/GetTextHtmlForPage` returns one
   page's OCR text in `.data.text`; the scraper loops over all pages and joins
   them, stripping the occasional `<a>` tag the endpoint wraps around
   OCR-detected URLs.
4. **Date** — parsed from `DATE DECIDED` / `DATE MAILED` (MM/DD/YYYY), then any
   `Month DD, YYYY` or MM/DD/YYYY in the body; `null` if none.

Documents are scanned originals, so the OCR text carries a small amount of
noise (occasional stray characters); the body content is fully captured.

## Usage

```bash
python bootstrap.py test-api             # connectivity + extraction test
python bootstrap.py bootstrap --sample   # ~12 samples (newest first)
python bootstrap.py bootstrap            # full pull (all rulings)
python bootstrap.py bootstrap-fast       # alias for full pull (VPS wrapper)
```

## Scope

- **Included:** Opinions of the Commission and Advices of Counsel (both
  doctrine), 1979–present.
- **Not included:** `Rulings > Ethics > Orders` (enforcement / adjudicative
  orders = case_law) — a separate document class; and the Gaming, Lobbying and
  Medical Marijuana Act ruling trees.

## License

[Public Domain (Pennsylvania state government edict / public record)](https://www.law.cornell.edu/uscode/text/17/105) — State Ethics Commission Opinions and Advices are official public records of a Pennsylvania state agency interpreting statute (65 Pa.C.S. § 1108(k) makes each a public record). Government-edict works, published for public use via the Commission's public Ethics eLibrary. Commercial use permitted; no attribution required.
