# US/WA-BIIA — Washington Board of Industrial Insurance Appeals, Significant Decisions

The **Board of Industrial Insurance Appeals** is the Washington agency that hears
appeals from orders of the Department of Labor & Industries under the Industrial
Insurance Act, Title 51 RCW — workers' compensation, occupational disease,
self-insurance, penalties, crime-victims compensation and WISHA safety citations.

Most of the Board's output is unpublished *Decisions and Orders*. A curated set is
designated **Significant Decisions** under RCW 51.52.160 and WAC 263-12-195; those
are the Board's precedential body of workers' compensation case law and are cited
in Washington practice as

> *In re Christopher Aalmo*, BIIA Dec., 87 4382 (1989)

This source collects the whole Significant Decision series — **~958 decisions,
1955 to the present** — with full text.

## Access

There is no API and no directory listing, but the Board publishes two static index
pages that between them enumerate every Significant Decision PDF:

| Page | Gives |
|------|-------|
| `/SDNameIndex.html` | one table row per decision: case name, docket number, year, PDF href |
| `/SDSubjectIndex.html` | the same decisions filed under the Board's own subject headings, with the published headnote |

Neither index is a superset of the other — four decisions appear only in the
subject index, and one name-index row lost its link — so both are parsed and
unioned. The subject index also supplies the subject headings and the headnote
text that the name index has no column for; 820 of the 958 decisions carry them.

Each decision is a born-digital PDF at `/SDPDF/{docket}.pdf` holding the published
headnote page followed by the complete Board order (findings of fact, conclusions
of law, the order itself and any dissent). Text extracts cleanly with the shared
`common.pdf_extract` helper — samples run 5K–88K characters.

> ⚠️ **Host note.** `www.biia.wa.gov` never completes a TLS handshake (connection
> error, not a 403). The apex host **`biia.wa.gov`** answers 200 for everything and
> is what the scraper uses. Do not "fix" the URL back to `www.`.

The crawl is partitioned by year, oldest first, with completed years checkpointed
to `data/wa_biia_checkpoint.json`, so a killed run resumes instead of
re-downloading the years it already wrote.

Two decisions in one appeal share a docket (`57009(1).pdf` and `57009(2).pdf` are
both *In re Bill Murray*, 57,009), so whatever the file name carries beyond the
docket is appended to the record id.

## Usage

```bash
python bootstrap.py test-api           # index reachable + probe decision extracts
python bootstrap.py bootstrap --sample # 15 decisions spread across 1955-present
python bootstrap.py bootstrap          # full pull
python bootstrap.py bootstrap-fast     # full pull (VPS wrapper alias)
```

## Record shape

```json
{
  "_id": "US-WA-BIIA-SD-57-009-2",
  "_source": "US/WA-BIIA",
  "_type": "case_law",
  "title": "In re Bill Murray (II), BIIA Dec., 57,009 (1984)",
  "text": "...full Board order...",
  "date": "1984-11-27",
  "url": "https://biia.wa.gov/SDPDF/57009(2).pdf",
  "court": "Washington State Board of Industrial Insurance Appeals",
  "jurisdiction": "US-WA",
  "docket_number": "57,009",
  "citation": "In re Bill Murray (II), BIIA Dec., 57,009 (1984)",
  "case_name": "Bill Murray (II)",
  "subjects": ["OCCUPATIONAL DISEASE (RCW 51.08.140) — Psychiatric conditions (mental/mental)"],
  "headnote": "A worker's acute reaction to job stress...",
  "significant": true,
  "year": 1984
}
```

`date` is the Board's signature date, read from the order's own `Dated: ...`
block and cross-checked against the index year (the body of an order quotes the
appealed Department order's date, which must not be mistaken for it).

## Related sources

- `US/WA-PERC` — Washington Public Employment Relations Commission (public-sector labor), distinct subject matter.
- `US/CA-WCAB`, `US/TX-DWC-AppealsPanel` — the same state workers-compensation appellate vein in other states.
- `US/VA-WCC` — blocked (host TCP-filtered from all available vantages).

## License

[Public domain — 17 U.S.C. § 105 / state government edict](https://www.law.cornell.edu/uscode/text/17/105) — decisions of a Washington
State adjudicative body are government edicts and are not subject to copyright.
No attribution required; commercial use permitted.
