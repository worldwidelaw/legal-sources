# BE/DBRC — Dienst van de Bestuursrechtscolleges (Flemish Administrative Courts)

Full text of the arresten (judgments) of the four Flemish administrative
courts administered by the DBRC, published at <https://www.dbrc.be/rechtspraak>.

| Court | Scope | Arresten |
|-------|-------|----------|
| Raad voor Vergunningsbetwistingen (RvVb) | planning-permit disputes — annulment, suspension, extremely-urgent suspension (UDN) | 17,326 |
| Handhavingscollege (HHC) | environmental enforcement fines | 1,133 |
| Raad voor Studievoortgangsbetwistingen (R.Stvb.) | higher-education study-progress disputes | 840 |
| Raad voor Verkiezingsbetwistingen (R.Verkb.) | local election disputes | 103 |

**Size:** 19,410 arresten, work-years 2009-10 → 2025-26 (as of 2026-08-02).

## Access

The listing is a plain server-rendered Drupal Views page with facets — no
authentication, no JavaScript, no WAF from a residential vantage.

| Step | Request |
|------|---------|
| Listing | `GET /rechtspraak?page={N}` — 20 judgment PDF links per page (~971 pages) |
| Listing by court | `GET /rechtspraak?f[0]=document_type:{id}&page={N}` |
| Document | `GET /sites/default/files/{YYYY-MM}/{COURT}[.{PROC}].{WORKYEAR}.{NNNN}.pdf` |

> Use **www.dbrc.be**, not the `www.rvvb.be` mirror — rvvb.be rewrites direct
> PDF requests into a JS-aggregator redirect.

### Implementation notes

- The walk is partitioned by the `document_type` facet (6 units covering
  19,402 of the 19,410 arresten), then finished with an unfaceted sweep that
  picks up the handful of arresten carrying no `document_type` term. Each
  record therefore arrives labelled with its court and procedure, and a
  restart does not re-walk completed units.
- Completed units, the in-progress page and the set of already-seen PDF URLs
  persist to `data/checkpoint.json`, so a relaunched fleet slot resumes
  without re-downloading PDFs it has already processed.
- Filenames follow `{COURT}[.{PROC}].{WORKYEAR}.{NNNN}.pdf` — e.g.
  `RVVB.UDN.2526.0627`, `RVVB.S.2526.0633`, `MHHC.M.1718.0069_0`,
  `RSTVB.2526.0870`, `RVERKB.2425.0041` — supplying court code, procedure,
  work-year and sequence. Decision number, roll number and pronouncement date
  are parsed from the arrest header (`ARREST van 27 maart 2026 met nummer
  RvVb-UDN-2526-0627 in de zaak met rolnummer 2526-RvVb-0613-UDN`).
- Documents are born-digital; text comes from PyMuPDF (fitz) with a
  pdfplumber fallback. No OCR path is needed.
- The site footer carries a `toegankelijkheidsverklaring.pdf`; links are taken
  from the result `<article>` cards and the regex fallback keeps only
  judgment-pattern filenames, so it never leaks into the corpus.
- If the listing returns no judgment links the run raises rather than
  reporting an empty corpus, so a future block fails loud.

## Usage

```bash
python bootstrap.py test-api             # connectivity + one full-text arrest
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full pull (resumable)
python bootstrap.py bootstrap-fast       # high-throughput full pull (fleet)
python bootstrap.py updates --since 2026-01-01
```

## Record schema

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `title`, `text`,
`date`, `url`, `decision_number`, `roll_number`, `court`, `court_code`,
`procedure`, `document_type`, `work_year`, `language`, `country`.

## License

> ⚠️ **Commercial use restricted.** See terms below.

[DBRC disclaimer](https://www.dbrc.be/disclaimer) — reproduction, publication
or any commercial reuse of the arresten requires the prior express written
consent of the Dienst van de Bestuursrechtscolleges. The judgments are freely
consultable for personal and non-commercial use.
