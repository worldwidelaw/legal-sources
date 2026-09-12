# US/CherokeeNationCourts — Cherokee Nation Supreme Court Opinions

Decisions of the **Supreme Court of the Cherokee Nation** and of its predecessor
the **Judicial Appeals Tribunal (JAT)**, published by the Nation's Judicial
Branch at [cherokeecourts.org](https://www.cherokeecourts.org/Supreme-Court).

The Cherokee Nation is a federally recognized sovereign tribe headquartered in
Tahlequah, Oklahoma, with its own three-branch government and its own courts.
The Supreme Court is the court of last resort; the Judicial Appeals Tribunal was
its equivalent before the 1999 Constitution took effect in 2003.

**401 decision documents across 359 dockets, 1978 → 2025.**

## What is collected

One document is one record. A docket's papers are filed and cited separately
("SC-17-07 37-Final Order 2-22-21"), so an opinion and the orders in the same
case are *not* merged — they share a `case_number` instead.

| `_type` | What | Count |
|---|---|---|
| `case_law` | Opinions, judgments, orders, minute orders, dismissals, decrees | ~362 |
| `doctrine` | The Court's `SC-AD` administrative directives, and the Supreme Court / District Court Rules | ~39 |

## How it works

The Court publishes a single hub page,
`/Supreme-Court/Supreme-Court-Case-Opinions-and-Information`, which links seven
era archives — `1975-1995`, `1996-1997`, `1998-2001`, `2002-2006`, `2007-2012`,
`2013-2016` and `SC-2017-01-to-current`. Each archive is a plain
`<li><a href="…pdf">docket + caption</a></li>` list. Walking the hub plus those
seven pages yields the whole published corpus: no search form, no pagination,
no session state.

Metadata comes from three places that corroborate each other:

* the **anchor text** — `JAT-96-02 James Stockton v. Cherokee Nation` — the case caption;
* the **file name** — `SC-19-03 13-Opinion 3-14-19.pdf` — docket, filing sequence,
  document type and date;
* the **body**, used to recover a date when the file name omits one (a handful
  read `SC-24-03 13 - Opinion.pdf`). DNN's own `?ver=` upload stamp is the last
  resort, since it records when the scan was posted, not when the Court signed it.

### ⚠️ Every PDF on this site is a scan — OCR is required

Checked across all eras (1995, 2007, 2019, 2025): each page carries exactly one
full-page image and a **zero-character text layer**. There is no born-digital
path. Text therefore comes from OCR through the shared `common/pdf_extract`
cascade — opendataloader → pdfplumber → pypdf → **tesseract**. The first three
return nothing here; tesseract is what produces the body, at roughly ten seconds
a document (so a full run is ~1 hour of CPU, not of network).

**The host running this scraper must have `tesseract` on `PATH`.** Without it
every record would be empty, so `fetch_all` raises rather than writing stubs:

```
0 of 401 documents yielded text — every PDF on this site is a scan, so this
almost always means tesseract/OCR is unavailable on this host
```

`_entries()` likewise raises if fewer than 100 documents are enumerated, so an
index layout change or an IP block fails loud instead of silently shrinking the
corpus.

## Coverage limits (deliberate)

* **District Court decisions are not published here.** Cherokee Nation District
  Court records live on a Tyler Technologies Odyssey portal
  (`portal-okcherokeenation.tylertech.cloud`), which is a docket-search
  application rather than a published-opinion corpus. Out of scope.
* **ODCR (odcr.com)** is a third-party for-profit aggregator whose terms state
  "You must be a human. Accessing the data by any automated mechanism is not
  permitted, unless using the API." Deliberately not a source — see issue #1499.
* The individual `/Supreme-Court/SC-YYYY-NN-Party-v-Party` pages are **pending-case
  dockets** carrying party filings (petitions in error, designations of record),
  not decisions of the Court. Only decision PDFs are collected.
* `CNCA_order_form.pdf` on the same site is a print order form for the annotated
  code, not a corpus document; it carries no docket number and is filtered out.

## Related sources

* `US/CherokeeNationCode` — the Cherokee Nation Code Annotated (legislation).
* `US/CherokeeNationAGOpinions` — Attorney General opinions (doctrine).
* `US/ChickasawNationCode`, `US/MashantucketPequotLaws`, `US/NavajoNationCourts` —
  sibling tribal corpora.

## Usage

```bash
python bootstrap.py test-api            # connectivity + one OCR'd record
python bootstrap.py bootstrap --sample  # 12 samples spread across 1978-2025
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # full pull, streams to data/records.jsonl
```

## License

[Public Domain — tribal government edict](https://www.cherokeecourts.org/Supreme-Court) —
opinions and orders of the Supreme Court of the Cherokee Nation and of the
Judicial Appeals Tribunal are judicial edicts of a sovereign tribal government,
published in full and without restriction on the Judicial Branch's own site.
Under the government edicts doctrine judicial opinions are not subject to
copyright. Freely reusable, including commercially. No attribution required.
