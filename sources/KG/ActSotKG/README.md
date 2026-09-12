# KG/ActSotKG — Kyrgyz Republic Judicial Acts Portal (act.sot.kg)

Full text of judicial acts issued by the courts of the Kyrgyz Republic and
published on the State Judicial Acts Portal <https://act.sot.kg/> under the
Kyrgyz judicial-transparency mandate.

Coverage: district, city, oblast (regional), inter-district, military and
administrative courts plus the Supreme Court of the Kyrgyz Republic
(Кыргыз Республикасынын Жогорку Соту), in criminal, civil, administrative
and economic matters. Acts include ТОКТОМ (resolution), ЧЕЧИМ (decision),
АНЫКТАМА (ruling), sentences and other judicial acts, in Kyrgyz and Russian.

**Size:** ~378,000 acts (37,815 listing pages × 10 rows) as of 2026-08-02,
id space 70..414,841, running from 2013 to the present.

## Access

The portal is plain server-rendered HTML — no authentication, no JavaScript,
no API key.

| Step | Request |
|------|---------|
| Listing | `GET /kg/search?caseno=&judge=all&side1=&side2=&from=&to=&actType=all&submit-act=Актылар&caseType=all&page={N}&sort=act.created&direction=asc` |
| Document | `GET /act/download/{act_id}.pdf` |

Each listing row is a `<tr id="/act/download/{id}.pdf">` carrying the case
number and its portal URL, the subject-matter category, the act's published
name, the presiding judge, the issuing court, the act-approval date and the
publication date. The act body itself is a born-digital PDF.

### Implementation notes

- The walk is sorted **ascending on `act.created`** so newly published acts
  append at the tail rather than shifting every page. Combined with the
  page checkpoint in `data/checkpoint.json`, a relaunched run resumes at the
  first unfinished page with no network calls for pages already done.
- The corpus (~378K acts) does not fit in one 100-hour fleet slot, so the
  checkpoint is what lets it finish across slots. It is flushed every 20 pages
  **and on SIGTERM/SIGINT** (which is how the fleet cap ends a run), and it
  deliberately lags 15 pages behind the page being yielded so the ~100 records
  still sitting in the writer's batch when the run is killed are re-fetched next
  time instead of being skipped over.
- A listing page still unreachable after 3 attempts is **not** silently dropped
  — it is recorded as a coverage gap (counted in the run stats and written to
  `status.yaml`), queued in the checkpoint, retried at the end of the run, and
  retried first on the next run. `act.sot.kg` times out under sustained
  crawling, and one lost listing page costs all 10 acts behind it.
- Connect and read timeouts are separate (15s / 60s). A flat 60s connect timeout
  with 5 attempts spent ~5.5 minutes on each dead page and burned roughly a
  third of the fleet slot doing nothing. The inter-page delay also adapts: it
  widens after a failure and relaxes back toward 1s after successes.
- The listing page size is fixed at 10; `limit` / `perPage` are ignored.
  Duplicate act ids appear across adjacent pages when the ordering ties on
  `act.created`, so ids are also de-duplicated in-run.
- Text is extracted with **pdfplumber first, on purpose**: PyMuPDF renders
  the Kyrgyz-specific letters `ө` and `ү` as detached glyphs on their own
  lines for these PDFs, which corrupts the text. `fitz` is only a fallback
  when pdfplumber returns nothing. All probed acts are born-digital, so no
  OCR path is needed.
- If page 1 of the listing is unreachable the run raises rather than
  reporting an empty corpus, so a future IP/geo block fails loud.
- Party names are already anonymised to initials by the publisher on newer
  acts; older acts carry full names as published.

### Relationship to KG/CourtActs

The successor portal `portal.sot.kg` (registered separately as
**KG/CourtActs**) is a JavaScript SPA backed by a REST API and only carries
recent acts. `act.sot.kg` remains fully served and holds the historical
corpus; the two are complementary and de-duplicated by `_id` prefix.

## Usage

```bash
python bootstrap.py test-api             # connectivity + one full-text act
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full pull (resumable)
python bootstrap.py bootstrap-fast       # high-throughput full pull (fleet)
python bootstrap.py updates --since 2026-01-01
```

## Record schema

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `title`, `text`,
`date`, `url`, `act_id`, `act_name`, `case_number`, `case_url`, `category`,
`court`, `judge`, `published_date`, `language`, `country`.

## License

[Open government data — Kyrgyz Republic judicial acts](https://act.sot.kg/) —
judicial acts of the Kyrgyz Republic are official state documents published
for public access under the judicial-transparency mandate. Court acts are
official edicts and carry no copyright; the portal imposes no terms
restricting reuse, including commercial reuse.
