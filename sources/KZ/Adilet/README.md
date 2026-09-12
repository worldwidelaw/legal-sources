# KZ/Adilet — Adilet Legal Information System (Kazakhstan)

**Source:** [https://adilet.zan.kz/](https://adilet.zan.kz/)
**Publisher:** Institute of Legislation and Legal Information (ИЗПИ), Ministry of Justice of the Republic of Kazakhstan
**Data types:** legislation
**Language:** Russian (Kazakh versions exist at `/kaz/docs/{code}`)
**Corpus:** ~229,000 documents, 1947–present

Consolidated Kazakh legal acts: the Constitution, codes, constitutional laws,
laws, presidential decrees, government resolutions, ministerial orders, akimat
decisions, Constitutional Court/Council normative rulings, and Eurasian Economic
Commission decisions in force in Kazakhstan. Text is the *consolidated* version
with amendment footnotes (Сноска).

## Access strategy

The `zan.gov.kz` REST API this source originally used (`POST /api/documents/search`,
`GET /api/documents/{id}/rus`) now returns nginx **403 to every request** from
every vantage tested, including with browser headers — see issue #1471. The
public portal host `adilet.zan.kz` is not blocked, so both discovery and full
text are read from it.

| Step | Endpoint |
|------|----------|
| Discovery | `GET /rus/index/docs/dt={year}-&rss=true&page={n}` — RSS rendering of the date-browse index, 10 items/page, total in `<description>` |
| Full text | `GET /rus/docs/{code}` — whole consolidated act as HTML |

`robots.txt` disallows `/rus/search/`, `/rus/list/docs/` and `/rus/archive/`.
The `/rus/index/docs/` browse path used here is **not** disallowed.

Full text is extracted from the `<div class="container_gamma text ...">` block via
a balanced-`div` scan and tag strip. Sizes range from a few kB for a ministerial
order to 886K chars for the Criminal Code.

### Notable field semantics

- `date` — **adoption** date, parsed from the requisites line (`... от 29 декабря 1995 г. N 2737`).
- `revision_date` — the index's `<pubDate>`, which carries the *last amendment* date.
  These differ widely: the 1995 Constitution is `date: 1995-08-30`, `revision_date: 2026-07-01`.
- `act_type` — classified from the requisites line, never the title first, because a
  Constitutional Court ruling *about* a code would otherwise be labelled a code.

The document fetch lives in `normalize()` so `bootstrap-fast`'s worker pool
overlaps the per-document downloads; `fetch_all()` only walks the index.
Discovery skips codes already present in storage, so an interrupted run resumes
without re-downloading.

## Usage

```bash
python bootstrap.py test              # connectivity + one document
python bootstrap.py bootstrap --sample  # 15 sample records
python bootstrap.py bootstrap-fast    # full corpus -> data/records.jsonl
python bootstrap.py update            # incremental
```

## License

[Official documents are not objects of copyright](https://adilet.zan.kz/rus/docs/K930001000_) under the
Kazakh Copyright Law (законодательные, административные и судебные документы are
excluded from copyright protection). Published as open government data by the
Ministry of Justice — [portal terms](https://adilet.zan.kz/rus/help). Commercial
use permitted; attribution to ИПС «Әділет» is expected.
