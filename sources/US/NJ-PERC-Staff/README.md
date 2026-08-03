# US/NJ-PERC-Staff — NJ PERC Director, Hearing Examiner & Appeal Board Decisions

Full-text **case law**: the **non-Commission adjudications** issued under the
New Jersey Public Employment Relations Commission (PERC) — decisions of the
**Director of Representation (D.R.)**, **Director of Unfair Practices (C.O.)**,
**Hearing Examiners / Hearing Officers (H.E. / H.O.)**, and the **PERC Appeal
Board (A.B.D.)**. Each resolves a specific contested case under the New Jersey
Employer-Employee Relations Act (N.J.S.A. 34:13A) = `case_law`. These complement
the Commission's final decisions (source **US/NJ-PERC**) with the staff- and
appeal-board-level tier.

- **~920 decisions**
- **Full text** extracted from each decision PDF
- **No CAPTCHA, no auth**

## Data source

Same **Lotus Domino** database as US/NJ-PERC
(`https://www.perc.state.nj.us/percdecisions.nsf`), but a different view —
`Issued Decisions Non PERC`. Unlike the Commission view (year-categorized), the
whole non-Commission set is returned by a single expanded request:

```
GET /percdecisions.nsf/Issued%20Decisions%20Non%20PERC?OpenView&Count=5000&ExpandView
```

(Use `Count=5000`; the set stabilizes at ~920. `Count=800` caps at 726.
`RestrictToCategory={year}` returns 0 here because the top-level categories are
decision-**type**, not year.)

Each row links a decision PDF (served under the sibling `Issued Decisions`
resource path — note the space):

```
/percdecisions.nsf/Issued Decisions/{UNID}/$File/{name}.pdf?OpenElement
```

`{UNID}` is the stable Domino document universal id (record key). The **citation**
(`D.R. NO. 2018-6`, `A.B.D. NO. 2008-1`, `H.E. NO. 95-12`) is read from the first
line of the decision body (more reliable than the filename, since some
attachments carry Domino temp names like `~1911780.pdf`). The issued date is
parsed from the `ISSUED: <Month DD, YYYY>` stamp.

> **Note:** the PDF host requires a browser `User-Agent` — plain requests receive
> HTTP 403. The scraper downloads bytes with a browser UA and hands them to
> `common.pdf_extract` (older scanned Appeal Board PDFs go through OCR).

## Usage

```bash
python bootstrap.py test-api            # connectivity + extraction check
python bootstrap.py bootstrap --sample  # ~12 sample records
python bootstrap.py bootstrap           # full pull (~920 decisions)
python bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## Record schema

| field | description |
|-------|-------------|
| `_id` | `US/NJ-PERC-Staff/{UNID}` |
| `_source` | `US/NJ-PERC-Staff` |
| `_type` | `case_law` |
| `citation` | decision citation (e.g. `D.R. NO. 2018-6`) |
| `title` | decision title (the citation) |
| `text` | full decision text (PDF extract) |
| `date` | issued date (ISO 8601) |
| `url` | link to the decision PDF |
| `jurisdiction` | `US-NJ` |

## License

[Public Domain — U.S. state government edict](https://www.law.cornell.edu/uscode/text/17/105) — decisions issued under the New Jersey Public Employment Relations Commission (Director, Hearing Examiner, Hearing Officer, and Appeal Board) are official works of a quasi-judicial New Jersey state government body and are not subject to copyright under the government-edicts doctrine. Free to use, including commercially. No attribution required.
