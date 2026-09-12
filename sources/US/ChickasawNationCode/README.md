# US/ChickasawNationCode — Chickasaw Nation Code and Constitution

Full text of the codified law of the **Chickasaw Nation**, a federally
recognized sovereign tribe headquartered in Ada, Oklahoma: the Constitution of
the Chickasaw Nation and the **21 titles of the Chickasaw Nation Code**,
split to section level.

- Index: <https://chickasaw.net/Our-Nation/Government/Chickasaw-Nation-Code.aspx>
- Code portal: <https://code.chickasaw.net/Table-of-Contents>
- Judicial Department: <https://judicial.chickasaw.net/>
- Data type: `legislation` · Language: English · Auth: none

Built for issue #1499 (tribal law inventory, follow-up to #1497).

## How it works

The Nation runs a dedicated code portal that serves each title as its own
born-digital PDF straight off a clean path — `code.chickasaw.net/Title-05`
returns `application/pdf` with no HTML wrapper — and links all 21 from a single
government page. The scraper walks that page once, appends the Constitution PDF
found on the constitution page and the District Court Rules PDF found on the
District Court page, then extracts text with PyMuPDF (falling back to the shared
`common.pdf_extract` cascade).

### Section-level splitting

Titles are split into one record per **section**, the unit the code is cited at
(`10 C.N.C. § 10-101.4`). That is possible because each title PDF prints every
section number twice, in two distinguishable cases:

```
Section 10-101.4          <- table of contents, mixed case
Definitions.

SECTION 10-101.4          <- the provision itself, upper case
DEFINITIONS.
```

Anchoring on the **upper-case** form lands on the body and skips the contents
listing entirely. Two format variants are handled: a few recent acts number
sections flat (`SECTION 11-102`, Wildlife Conservation Act of 2022) instead of
chapter-then-section, and some titles print the caption on the anchor line
itself (`SECTION 20-100.1  TITLE.`). The anchor's own title number is checked
against the title being parsed, so a cross-reference printed at the start of a
line cannot be mistaken for an anchor; where a number still appears twice, the
longer body wins.

Chapter and article context is carried down from the nearest `CHAPTER n` /
`ARTICLE X` heading above the section.

### Coverage

| | |
|---|---|
| Code titles | 21 (Titles 1–21) |
| Sections | **~1,956** |
| Whole-title records | 3 — Titles 4, 9 and 14 are entirely `(RESERVED)` and have no sections to split |
| Constitution | 1 record, full text (~31K chars) |
| Largest title | Title 5 (Courts and Procedures), 501 pages → 608 sections |

Dates come from the legislative history each section prints at its end —
`(TL11-003, 12/17/93; PR29-006, 8/17/12)`, whose most recent date is when that
section was last amended. A section with no history falls back to the title's
own `(Amended as of MM/DD/YYYY)` line, and that in turn to the PDF's own build
date. Two-digit years are read as 19xx/20xx around a 2030 pivot.

## Known limits

- **No case law.** The Chickasaw Nation Judicial Department does not publish
  court opinions online — there is no opinions page and no public docket portal
  on `judicial.chickasaw.net`. District Court dockets back to 1992 are carried
  by **ODCR** (`odcr.com`), a third-party for-profit aggregator; it is *not*
  collected here because it is not the official publisher, its terms are
  restrictive, and its records are docket entries rather than published
  opinions. This source is legislation only. See issue #1499 for the audit.
- **District Court Rules PDF is a scan** with no text layer (16 pages, 15
  extractable characters). It is fetched and kept only where OCR is available
  in the environment, otherwise skipped with a warning. No coverage is lost:
  the court rules are enacted in Code Title 5, which is collected in full.
- `5 C.N.C. § 5-101.6` is a ~97K-character record because the Rules of
  Professional Conduct and Canons of Judicial Ethics are annexed to that one
  section in the PDF under `Rule N` headings rather than `SECTION` anchors.

## Usage

```bash
python bootstrap.py test-api              # connectivity check
python bootstrap.py bootstrap --sample    # 12 sample records
python bootstrap.py bootstrap             # full pull (~1,960 records)
python bootstrap.py bootstrap-fast        # full pull, streams to data/records.jsonl
```

## Record shape

```json
{
  "_id": "CNC-17-800-5",
  "_source": "US/ChickasawNationCode",
  "_type": "legislation",
  "title": "17 C.N.C. § 17-800.5 — Eluding An Officer",
  "citation": "17 C.N.C. § 17-800.5",
  "section": "17-800.5",
  "chapter": "8",
  "article": null,
  "section_heading": "Eluding An Officer",
  "title_number": 17,
  "title_name": "Offenses and Penalties",
  "date": "2020-11-20",
  "text": "...",
  "url": "https://code.chickasaw.net/Title-17",
  "jurisdiction": "US-CHICKASAW-NATION",
  "publisher": "The Chickasaw Nation"
}
```

## License

[Public Domain — tribal government edict](https://code.chickasaw.net/Table-of-Contents) —
the Chickasaw Nation Code and the Constitution of the Chickasaw Nation are
enacted law of a sovereign tribal government, published in full and without
restriction by the Nation's own code portal for public access. Under the
government edicts doctrine enacted law is not subject to copyright. Freely
reusable, including commercially.
