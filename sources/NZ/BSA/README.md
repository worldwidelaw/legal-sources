# NZ/BSA — Broadcasting Standards Authority Decisions

Complaint decisions of the New Zealand Broadcasting Standards Authority
(Te Mana Whanonga Kaipāho), an independent Crown entity that determines
complaints about television and radio broadcasts under the Broadcasting Act 1989.

- **Country:** NZ
- **Type:** `case_law`
- **Coverage:** 1990 – present, ~4,530 decisions
- **Auth:** none
- **Site:** https://www.bsa.govt.nz/decisions/

## Access strategy

The BSA publishes no API, bulk download, or SPARQL endpoint, so the decision
index is read from the site's own paginated listing.

1. **Enumerate** — `/decisions/all-decisions/?start=N` returns 10 decisions per
   page, newest first. The pagination block advertises the final `start` offset,
   which gives the expected corpus size up front. A walk that ends materially
   short of that count records a coverage gap instead of reporting success.
2. **Fetch** — each decision is a standalone HTML page. Metadata comes from the
   labelled `decision-details` boxes (members, complainant, broadcaster,
   programme, channel, standards) and the ISO date from the `<time datetime>`
   attribute.
3. **Extract text** — two eras, and both must be handled:

   | Era | Page shape | Text source |
   |---|---|---|
   | ~1994 – present | Full decision rendered in-page | `wysiwyg-content` block |
   | 1990 – ~1993 | Stub page: "Download a PDF of Decision No. X" | Born-digital PDF |

   The earliest ~380 decisions carry **no in-page text at all** — roughly 120
   characters of download link. An HTML-only scraper would land them as
   empty-text rows while still exiting 0. `normalize()` therefore falls back to
   PDF extraction whenever the in-page body is under 1,200 characters. The
   legacy PDFs are born-digital and extract cleanly (8K–32K chars); no OCR is
   required.

   Note that the legacy PDF href is site-root-relative *without* a leading slash
   (`images/assets/PDF-Decisions/...`). Resolving it against the decision page
   URL yields a 404 — it must be joined against the site root.

## Sampling

`bootstrap --sample` strides evenly across the pagination range rather than
taking the newest page, so the committed samples span 1991–2026 and exercise
both the HTML and PDF-only eras. Sampling only the head would validate the easy
half of the corpus and miss exactly where this source breaks.

## Usage

```bash
python bootstrap.py test-api             # Connectivity + parse check
python bootstrap.py bootstrap --sample   # 15 samples spanning the corpus
python bootstrap.py bootstrap            # Full pull
python bootstrap.py bootstrap-fast       # Fleet entry point
```

## Record shape

`_id`, `_source`, `_type`, `_fetched_at`, `title`, `text`, `date`, `url`,
`decision_number`, `court`, `jurisdiction`, `members`, `complainant`,
`broadcaster`, `programme`, `channel`, `standards`, `standards_breached`,
`pdf_url`, `language`.

`_id` is `bsa-{decision number}` (e.g. `bsa-2026-019`), falling back to the URL
slug when a decision carries no number.

## Rate limiting

1 request/second, burst 2. The full crawl is ~4,980 requests
(453 listing pages + 4,530 decisions) plus ~380 legacy PDF downloads.

## License

[CC BY 4.0](https://creativecommons.org/licenses/by/4.0/) — attribution required.

The BSA is a New Zealand Crown entity. Under
[NZGOAL](https://www.data.govt.nz/toolkit/policies/nzgoal/), the New Zealand
Government Open Access and Licensing framework, State Services agencies release
copyright works for re-use under Creative Commons attribution licences by
default. The site publishes no narrower notice. Commercial use is permitted.
