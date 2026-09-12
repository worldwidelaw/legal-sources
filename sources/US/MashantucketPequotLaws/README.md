# US/MashantucketPequotLaws — Mashantucket Pequot Tribal Laws (M.P.T.L.)

The complete codified body of law of the **Mashantucket (Western) Pequot Tribal
Nation**, a federally recognized sovereign tribe in Connecticut. Together with
`US/NavajoCourts` this covers the other half of tribal law: where Navajo gives
tribal *case law*, this gives tribal *enacted law* — a legal system that is
neither federal nor state and that mainstream US legal databases do not carry.

- **Publisher:** Mashantucket Pequot Tribal Court (law portal)
- **Index:** https://law.mptn-nsn.gov/tribal-laws/
- **Type:** `legislation`
- **Language:** English
- **Size:** 60 official documents — the Tribal Constitution & Bylaws, the Rules
  of Court, ~53 numbered M.P.T.L. titles, the bound 2008 code volumes with
  their 2009–2014 pocket parts, and the dated current supplement — split into
  roughly 1,500 provision-level records

## Access strategy

There is no API or bulk endpoint. The law portal publishes every enacted title
as its own official PDF and links them all from a single static index page:

```html
<a href="/globalassets/laws/title-3-gaming.pdf">TITLE 3 GAMING</a>
```

so the scraper walks that index once and downloads each `/globalassets/laws/*.pdf`
link. **Only that directory is collected** — the same page also links practice
forms and e-filing standing orders from `/globalassets/` proper, which are not
enacted law.

## Provision-level splitting

Each PDF prints the official citation of a provision on a line of its own
immediately above that provision:

```
2 M.P.T.L. ch. 1 § 2          <- numbered titles
§ 2. Definitions

M.P.R.C.P. 3                  <- rules volume
Rule 3. Commencement of Action
```

Those anchor lines are used as split points, so **one record is one section,
rule or canon** — the unit the code is actually cited at, and a far better
retrieval unit than a 100,000-character title. The rules volume bundles five
separately cited codes (`M.P.R.C.P.`, `M.P.R.E.`, `M.P.R.A.P.`, `M.P.J.C.`,
`M.P.L.C.C.`) and splits into all of them.

M.P.T.L. anchors are matched only for the title's *own* number, so a
cross-reference to another title printed at the start of a line is not mistaken
for an anchor. A PDF with no anchors at all — a `RESERVED` placeholder, the
scanned constitution, or Title 13, which predates the citation scheme and uses
`CHAPTER 1. / Section 1.` headings instead — is emitted as one whole-document
record rather than dropped.

`compilation` and `supplement` documents are deliberately **not** split: they
restate the same citations in superseded form, and splitting them would put two
different texts under one citation.

Documents are labelled in `document_type`:

| `document_type` | What it is |
|---|---|
| `title` | A single numbered M.P.T.L. title, current text (e.g. `3 M.P.T.L.`) |
| `constitution` | Tribal Constitution & Bylaws |
| `rules_of_court` | Mashantucket Pequot Rules of Court |
| `compilation` | Bound 2008 code volumes and their 2009–2014 pocket parts |
| `supplement` | Dated current supplement to the M.P.T.L. |

The historical compilations are kept rather than dropped as duplicates: they are
the official text *for their period*, and their content is not identical to
today's titles.

Full text comes from PyMuPDF, with the shared `common.pdf_extract` cascade
(opendataloader → pdfplumber → pypdf → OCR) as the fallback. PyMuPDF is primary
because the citation anchors above only survive as standalone lines in its raw
text output. Nearly everything is born-digital — an individual title runs
30K–50K characters and the bound volumes exceed 1M.

## Dates

The portal publishes no effective-date field. Where a file name records the
Tribal Council resolution that last amended a title
(`...-current-as-of-tcr072723-01.pdf` = TCR of 2023-07-27) that date is used, as
is the date in the constitution's label and the supplement's file name — those
land in `amended_by_tcr` as well as `date`. Otherwise `date` falls back to the
date the PDF itself was produced, i.e. when the Office of Legal Counsel last
recompiled that title.

## Coverage limits

- The **Constitution & Bylaws PDF is a 7 MB scan with no text layer**, so it is
  captured only on hosts where the tesseract OCR fallback is available.
- The tribe's **case law is not open**: reported decisions are published as
  *West's Mashantucket Pequot Reports*, a commercial Thomson Reuters title. Only
  the codified laws are freely available, so this source is legislation-only.

## Usage

```bash
python bootstrap.py test-api             # connectivity + one extraction
python bootstrap.py bootstrap --sample   # ~12 sample records
python bootstrap.py bootstrap            # full pull
python bootstrap.py bootstrap-fast       # full pull streamed to data/records.jsonl
```

## Record shape

| Field | Notes |
|---|---|
| `_id` | `MPTL-t{title}-ch{chapter}-s{section}` (e.g. `MPTL-t2-ch1-s2`), `MPTL-m-p-r-c-p-3` for rules, or `MPTL-{file-slug}` for unsplit documents |
| `_type` | `legislation` |
| `title` | `{citation} — {heading}`, e.g. `2 M.P.T.L. ch. 1 § 2 — Definitions` |
| `text` | Full text of the provision, heading included |
| `date` | TCR amendment date where derivable, else the PDF's build date |
| `document_type` | See table above |
| `document` | Parent document's index label, e.g. `TITLE 3 GAMING` |
| `title_number` | Integer title number for `title` records |
| `citation` | Official citation, e.g. `3 M.P.T.L. ch. 1 § 2` |
| `section_heading` | Section/rule heading, e.g. `Definitions` |
| `amended_by_tcr` | Date of the Tribal Council resolution named in the file name, else null |
| `url` | Official PDF |

## License

[Public Domain — tribal government edict](https://law.mptn-nsn.gov/tribal-laws/) —
the Mashantucket Pequot Tribal Laws are enacted law of a sovereign tribal
government, published in full by the Tribal Court's law portal for public
access. Under the government edicts doctrine enacted law is not subject to
copyright. Commercial use permitted.

> Note: *West's Mashantucket Pequot Reports* (the tribe's reported case law) **is**
> a copyrighted commercial publication. It is deliberately out of scope here.
