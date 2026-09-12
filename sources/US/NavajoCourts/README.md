# US/NavajoCourts — Navajo Nation Supreme Court, Opinions

Full-text published slip opinions of the **Supreme Court of the Navajo Nation**,
the highest court of the largest tribal judiciary in the United States. The
Navajo Nation reservation spans parts of Arizona, New Mexico and Utah, and its
Judicial Branch operates the busiest tribal court system in the country. This is
the project's first source of *tribal* case law — a body of law that is neither
federal nor state, and that is absent from the mainstream US legal databases.

- **Publisher:** Judicial Branch of the Navajo Nation
- **Index:** https://courts.navajo-nsn.gov/suctopinions.htm
- **Type:** `case_law`
- **Language:** English
- **Coverage:** 2013–2021 (55 distinct opinions), plus two standing court
  orders from 1995 and 2004

> **Host note.** The official host is `courts.navajo-nsn.gov`. The
> `navajocourts.org` domain the court used historically has lapsed and now
> redirects to an unrelated squatted domain — do not point anything at it.

## Access strategy

There is no API, open-data portal, ELI scheme or bulk download for this court;
the Judicial Branch publishes a single static HTML index (`/suctopinions.htm`,
FrontPage-era markup) linking one PDF per opinion. That index is the only access
path, so this source is a deliberate HTML-scrape.

The index is unusually rich for a static page — each opinion is a three-cell
table row carrying the docket number, case caption and decision date:

```html
<td>1.</td>
<td><a href="NNSC2017/01Arviso-v-Muskett.pdf">SC-CV-18-17</a></td>
<td><u>Kathleen Arviso v. Norma Muskett</u>. <i>Opinion</i>. (April 5, 2017).</td>
```

so no per-case detail fetch is needed for metadata. Two details matter:

- The **docket number lives in the anchor text, not the file name.** File naming
  changed era to era (`SC-CV-18-17.pdf` recently, `01Arviso-v-Muskett.pdf` in
  2017–2018), so reading the docket off the URL silently mislabels whole years.
- A few opinions are **linked twice** (e.g. `NNSC2020/SC-CV-13-15.pdf` and
  `NNSC2020/SC-CV-13-15, Opinion.pdf`), so entries are deduplicated on
  *year-directory + docket*, not on the URL.

Full text comes from the opinion PDFs via the shared `common.pdf_extract`
cascade (opendataloader → pdfplumber → pypdf → PyMuPDF → tesseract OCR).

## Coverage limits

- **~40% of the index is scanned image-only** — all of 2021, all of 2018, and
  scattered opinions in 2013–2020 have no text layer. These are captured only on
  hosts where the tesseract OCR fallback is available; 32 of 55 opinions extract
  from a text layer alone.
- **Pre-2013 opinions are not online.** They were published only in the print
  *Navajo Reporter*, which the court sells via a mail order form.
- **No opinions after 2021** exist on the site. `NNSC2022/` and later return
  HTTP 404 and directory listing is disabled (403), so the index is the complete
  online corpus.
- The court's own year-by-year **opinion summaries** (`NNSCSummaries/2012.html`,
  `2013.html`) are intentionally **not** collected. They carry an explicit
  disclaimer that they are educational paraphrases which "may not be relied on or
  otherwise cited in legal proceedings" — they are not the opinions themselves.

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
| `_id` | `NavajoCourts-{yeardir}-{docket}`, e.g. `NavajoCourts-NNSC2017-SC-CV-18-17` |
| `_type` | `case_law` |
| `title` | Case caption, with the docket appended when not already present |
| `text` | Full opinion text |
| `date` | Decision date, ISO 8601 (null where the index gives only a year) |
| `case_number` | Docket, e.g. `SC-CV-18-17` |
| `case_name` | Underlined caption from the index |
| `court` | `Navajo Nation Supreme Court` |
| `url` | Opinion PDF |

## License

[Public Domain — tribal government edict](https://courts.navajo-nsn.gov/disclaimer.htm) —
opinions of the Navajo Nation Supreme Court are edicts of a sovereign tribal
government, published as public records by the Judicial Branch for public
access. Under the government edicts doctrine judicial opinions are not subject
to copyright. The site disclaimer limits the court's warranty of accuracy; it
does not restrict reuse. Commercial use permitted.
