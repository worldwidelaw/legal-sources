# US/CA-Courts — California State Courts

**Source:** [https://courts.ca.gov/](https://courts.ca.gov/) via the
[CourtListener API](https://www.courtlistener.com/help/api/rest/)
**Data types:** case_law

## Coverage

Audited live on 2026-08-26 with `bootstrap.py coverage`:

| Court | CourtListener id | Opinions available |
|---|---|---|
| Supreme Court of California | `cal` | 44,150 |
| Court of Appeal of California | `calctapp` | 114,861 |
| California Superior Court (published) | `calsuperct` | 485 |
| **Total** | | **159,496** |

## Enumeration

The corpus is swept **per court, per year, newest year first**, and every
completed `(court, year)` window is written to `data/checkpoint.json`. Runs
therefore resume where the previous one stopped.

This replaced a single flat `dateFiled desc` cursor walk that restarted from
the newest opinion on every run, which is why only 7,378 rows (2018–2026) had
ever been indexed (issue #1493). The old full path also wrote each record to
`sample/` instead of streaming to `data/records.jsonl`, and did not recognise
the `bootstrap-fast` command the fleet wrapper invokes — both made a full run
look like a sample-only run downstream.

## Full text availability

A cluster with no resolvable text is reported, never silently dropped. Text is
resolved through a ladder:

1. **Opinions API** (`/api/rest/v4/opinions/{id}/`) — `html_with_citations`,
   `html`, `html_lawbox`, `html_columbia`, `html_anon_2020`, `xml_harvard`,
   `plain_text`. **Requires `COURTLISTENER_API_TOKEN`**; anonymous callers get
   HTTP 401.
2. **Stored file** on `storage.courtlistener.com` (`local_path`), else the
   court's own `download_url`.

California opinions filed from roughly **2013 onward** carry a
`pdf/YYYY/MM/DD/*.pdf` `local_path` and are retrievable anonymously. Earlier
opinions are API-only:

* most pre-2013 clusters have `local_path` **and** `download_url` both null;
* a subset carries a legacy `california/..._opinions/documents/*.xml`
  `local_path` inherited from the Public.Resource.Org import — every one of
  those returns HTTP 404 from `storage.courtlistener.com` today, so the scraper
  treats the prefix as dead rather than counting it as an extraction failure.

**Without a token the backfill can only reach roughly the 2013–present slice
(~20K opinions). Set `COURTLISTENER_API_TOKEN` to ingest the remaining ~140K.**
Every skipped cluster is tallied by reason in `data/coverage_report.json`
(`skipped_no_stored_text`, `skipped_extraction_failed`, `skipped_no_opinions`,
`search_failures`), so expected-versus-fetched is auditable after each run.

A free token is available at
[courtlistener.com/profile/api](https://www.courtlistener.com/profile/api/).

## Usage

```bash
python bootstrap.py test                      # connectivity
python bootstrap.py coverage                  # live expected-count audit
python bootstrap.py bootstrap --sample        # 15 samples across courts/years
python bootstrap.py bootstrap --full          # checkpointed backfill
python bootstrap.py bootstrap-fast --full     # alias used by the fleet wrapper
python bootstrap.py update --since 2026-01-01 # incremental
```

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105)
