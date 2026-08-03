# UK — Local Government & Social Care Ombudsman (LGSCO) Decisions

Published investigation decisions of the **Local Government and Social Care
Ombudsman** (the Commission for Local Administration in England, established
under the Local Government Act 1974). The Ombudsman investigates complaints
about councils and registered adult social care providers, and — jointly with
the Parliamentary and Health Service Ombudsman — some health matters.

Each decision records the Ombudsman's findings (fault / no fault /
maladministration causing injustice), the injustice caused, and any remedy the
body has agreed to. These are the authoritative, published outcomes of the
statutory complaints jurisdiction = **case_law**.

- **Corpus:** ~57,500 decisions (bulk 2016–present).
- **Categories:** adult social care, children's care, housing, education,
  benefits & tax, environment & regulation, planning, transport & highways,
  health, and other.
- **Full text:** born-digital HTML, no OCR.

## How it works

1. **Enumeration** — the public decisions listing is a server-rendered search:
   `GET /Decisions/SearchResults?fd=YYYY-MM-DD&td=YYYY-MM-DD&page=N`
   (10 results/page; total shown as "Your search has N results"). The site's
   front-end posts `decisionSearch` to `/decisionsnew/newsearchpost`, which
   302-redirects to this GET results URL — the scraper calls the GET directly.
   There is **no reCAPTCHA on the results endpoint**; the captcha only guards
   the site-wide `/searchpost` and the complaint forms.
2. **Date windows** — enumeration runs one calendar month at a time so each
   window stays well under any pagination ceiling. Completed windows are
   checkpointed to `data/lgo_checkpoint.json` so fleet re-runs resume without
   re-fetching.
3. **Full text** — each result links to `/decisions/{category}/{subcategory}/{ref}`
   (ref = `YY NNN NNN`). The `<article id="article">` holds the metadata header
   (authority + reference, Category, Decision outcome, Decision date) followed by
   the narrative (Summary / The complaint / The Ombudsman's role and powers /
   How I considered this complaint / My assessment / Final decision).

## Usage

```bash
python bootstrap.py test                # connectivity check
python bootstrap.py bootstrap --sample  # 12 sample records
python bootstrap.py bootstrap           # full pull
python bootstrap.py update              # incremental (recent months)
```

## Record schema

`_id`, `_source` (`UK/LGO`), `_type` (`case_law`), `_fetched_at`, `case_id`
(reference, e.g. `24 011 791`), `title`, `text` (full decision), `date`
(ISO 8601), `authority` (the council / care provider), `category`,
`subcategory`, `outcome`, `url`.

## License

[Commission for Local Administration in England — custom re-use terms](https://www.lgo.org.uk/copyright)
— "You may re-use the information on this website free of charge in any format"
(copying, publishing, broadcasting, translating), subject to acknowledging the
source, reproducing accurately, and **not** using it "for the principal purpose
of advertising or promoting a particular product or service". OGL-like;
commercial use permitted with attribution.
