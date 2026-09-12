# GE/CommonCourts-Decisions — Georgia, Common Courts Unified Decisions Database

Full text of decisions of the common courts of Georgia published in the
unified electronic decisions database at <https://ecd.court.ge/>.

| Instance | Decisions |
|----------|-----------|
| პირველი ინსტანცია (first instance — city & district courts) | 30,638 |
| სააპელაციო (courts of appeal) | 11,892 |
| საკასაციო (cassation) | 1,959 |
| **Total** | **44,489** |

Case categories: სისხლის სამართლის (criminal), სამოქალაქო (civil),
ადმინისტრაციული (administrative). Text is Georgian; party names are
pseudonymised upstream by the publisher.

Distinct from **GE/SupremeCourt-Decisions** (supremecourt.ge — different host
and corpus).

## Access

Undocumented JSON API — no authentication, no captcha, no WAF. Both endpoints
are form-encoded and require `X-Requested-With: XMLHttpRequest`.

| Step | Request |
|------|---------|
| Listing | `POST /Decision/DecisionDocuments` — `Skip`, `Take` (≤50), `InstanceId`, `CaseCategoryId`, `DecisionDateFrom`, `DecisionDateTo` → `{"data":{"Total":N,"Items":[…]}}` |
| Full text | `POST /Decision/DecisionDocumentText` — `InstanceId`, `DecisionDocumentId` → `data.RawData` |
| PDF | `POST /Decision/DecisionDocumentPdf` (not used — RawData is complete) |

### Implementation notes

- **`Take` caps at 50 and `Skip` caps at ~10,000** (higher values return HTTP
  500), so the corpus cannot be walked as one stream. `fetch_all()` partitions
  it into `InstanceId × CaseCategoryId × decision-date window` units and
  recursively bisects any window whose `Total` exceeds the safe skip ceiling
  (9,500), so every record stays reachable.
- **`DecisionDateTo` *is* honoured**, contrary to the original research note.
  Verified live 2026-08-02 for `InstanceId=1`/`CaseCategoryId=1`: unbounded
  10,375 vs `To=01.01.2019` → 4,759, `From=01.01.2019&To=01.01.2020` → 4,796,
  `From=01.01.2020` → 821 (10,376 — one record double-counted on the shared
  boundary). Boundary duplicates are de-duplicated on `_id`.
- Category totals within the first instance (10,375 + 11,980 + 8,283 = 30,638)
  match the instance total, confirming the category space is exactly {1, 2, 3}.
- Completed `(instance, category, window)` units and the in-progress skip
  offset persist to `data/checkpoint.json`, so a relaunched fleet slot resumes
  at the first unfinished unit.
- Listing items already carry the court, instance, case number, category,
  decision type, barcode and date, so nothing has to be parsed out of the
  decision body.
- If the unfiltered listing returns no results the run raises rather than
  reporting an empty corpus, so a future block fails loud.

## Usage

```bash
python bootstrap.py test-api             # connectivity + one full-text decision
python bootstrap.py bootstrap --sample   # 15 sample records
python bootstrap.py bootstrap            # full pull (resumable)
python bootstrap.py bootstrap-fast       # high-throughput full pull (fleet)
python bootstrap.py updates --since 2026-01-01
```

## Record schema

`_id`, `_source`, `_type` (`case_law`), `_fetched_at`, `title`, `text`,
`date`, `url`, `decision_document_id`, `case_id`, `case_number`, `court`,
`court_code`, `instance`, `instance_id`, `case_category`, `document_type`,
`barcode`, `created_date`, `language`, `country`.

## License

[Open government data — decisions of the common courts of Georgia](https://ecd.court.ge/)
— court decisions are official state acts published for public access under
the Georgian judicial-transparency rules. The database requires no
registration and imposes no terms restricting reuse, including commercial
reuse. Party names are pseudonymised upstream by the publisher.
