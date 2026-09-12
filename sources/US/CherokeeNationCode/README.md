# US/CherokeeNationCode — Cherokee Nation Code Annotated

The codified law of the **Cherokee Nation**, published by the Nation's Office of
the Attorney General at
[attorneygeneral.cherokee.org/tribal-code](https://attorneygeneral.cherokee.org/tribal-code/).

The Cherokee Nation is the largest federally recognized tribe in the United
States — roughly 450,000 citizens and a reservation covering fourteen counties of
north-eastern Oklahoma — with its own three-branch government, its own courts and
its own code.

**One record is one section**, the unit the code is cited at (`21 CNCA § 1289.6`),
plus four whole-document constitutional instruments.

## What is collected

| `document_type` | What |
|---|---|
| `instrument` | The Act of Union of 1839 and the Constitutions of 1839, 1975 and 1999 |
| `section` | Every section of the 85 titles of the Cherokee Nation Code Annotated |

All records are `_type: legislation`.

## How it works

The Attorney General publishes the code as **five born-digital PDFs**, all linked
from one static download listing:

| File | What |
|---|---|
| `word-searchable-full-code.pdf` | The consolidated code — 1,788 pages, ~4.6M characters |
| `title-21-amendments-…pdf` | Crimes and Punishments, as amended 2021 |
| `title-10a-amendment-…pdf` | Juvenile Code, as amended 2021 |
| `title-22-amendment-…pdf` | Criminal Procedure, as amended 2021 |
| `title-47-amendments.pdf` | Motor Vehicles, as amended 2021 |

There is no API, no search form and no pagination — the listing is walked once
and each PDF is read straight through. The media paths are **not** hard-coded:
the Nation re-uploads under fresh Umbraco hashes (`/media/5upcrg3j/…`), and the
listing is also where each file's `Created:` / `Updated:` stamp lives, which is
where record dates come from. The code prints no per-section legislative history,
so a section's date is the date of the volume it was published in.

Every PDF is **word-searchable** — PyMuPDF returns a clean text layer, so there is
no OCR anywhere in this source and extraction is deterministic.

### Splitting to section level

The consolidated PDF is unusually regular and, crucially, carries **no
per-chapter contents listing**, so a section number appears exactly once and an
anchor always lands on the provision rather than on a table of contents:

```
TITLE 21                 <- title anchor, alone on its line
CRIMES AND PUNISHMENTS   <- title name, the next non-blank line
CHAPTER 1                <- chapter context, carried down
§ 1. Title of code       <- section anchor; the body runs to the next anchor
```

Anchors are line-anchored because cross-references are written inline
(`under 1 CNCA § 317`) and so never start a line. Title, part, article and
chapter context is carried down from the most recent heading above the section —
that context is what distinguishes `21 CNCA § 1` from `22 CNCA § 1`, since
section numbers restart in every title.

Each page of the consolidated PDF prints a bare page number as its first line;
left in, it would land inside whichever section spans the page break, so it is
stripped before splitting.

### ⚠️ The amendment volumes are kept alongside the consolidated code

The consolidated volume is the 2019 edition. Titles 10A, 21, 22 and 47 were
re-published in amended form in 2021, and **both texts are collected** rather
than one silently overwriting the other: they are separately published documents,
they get distinct `_id`s and `url`s, and each record carries the `document` it
came from and that volume's own `date`. A consumer wanting current text for those
four titles should take the later `date`.

This is deliberate. The amendment volumes carry the Nation's own notice that they
"have not been officially codified", so treating them as a silent replacement for
the codified text would assert more than the publisher does.

## Coverage note — code, not statutes-at-large

The AG's page says *"Please refer to the statutes-at-large as the authoritative
text for specific code provisions"*, and the amendment volumes point at
[cherokee.legistar.com](https://cherokee.legistar.com/Legislation.aspx).

This source is the **code** — the consolidated statement of law. The Legistar
acts database (individual Legislative Acts as enacted) is a different corpus and
is deliberately out of scope here.

## Record shape

```json
{
  "_id": "CNCA-word-searchable-full-code-29-112",
  "_source": "US/CherokeeNationCode",
  "_type": "legislation",
  "title": "29 CNCA § 112 — Enforcement and field citations",
  "citation": "29 CNCA § 112",
  "title_number": "29",
  "title_name": "Game and Fish",
  "chapter": "1",
  "section": "112",
  "section_heading": "Enforcement and field citations",
  "document_type": "section",
  "date": "2019-05-29",
  "text": "…",
  "url": "https://attorneygeneral.cherokee.org/media/5upcrg3j/word-searchable-full-code.pdf"
}
```

## Related sources

* `US/CherokeeNationCourts` — Cherokee Nation Supreme Court and JAT decisions (case law).
* `US/CherokeeNationAGOpinions` — Attorney General opinions (doctrine).
* `US/ChickasawNationCode`, `US/MashantucketPequotLaws`, `US/NavajoNationCourts` —
  sibling tribal corpora.

## Usage

```bash
python bootstrap.py test-api            # connectivity + one normalized record
python bootstrap.py bootstrap --sample  # 12 samples spread across the code
python bootstrap.py bootstrap           # full pull
python bootstrap.py bootstrap-fast      # full pull, streams to data/records.jsonl
```

## License

[Public Domain — tribal government edict](https://attorneygeneral.cherokee.org/tribal-code/) —
the Cherokee Nation Code Annotated, the Act of Union and the Constitutions of the
Cherokee Nation are enacted law of a sovereign tribal government, published in
full and without restriction by the Nation's own Office of the Attorney General.
Under the government edicts doctrine enacted law is not subject to copyright.
Freely reusable, including commercially. No attribution required.
