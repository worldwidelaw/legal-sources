# US/MA-SessionLaws — Massachusetts Session Laws (Acts & Resolves)

Full text of the chronological **Acts and Resolves** enacted by the General
Court of Massachusetts since **1692**, sourced from the State Library of
Massachusetts' open DSpace repository.

This is the *as-enacted*, chapter-by-chapter legislative record — one document
per `Chap. NNNN. An Act ...` / `Resolve ...`. It is distinct from
**US/MA-Legislation**, which covers only the *current consolidated* General Laws
(malegislature.gov).

## Access

- **DSpace 7 REST API** — `https://archives.lib.state.ma.us/server/api`
  (open, no authentication, no WAF/CAPTCHA).
- **Enumeration** — Discover search filtered to subject
  `Session Laws - Massachusetts` (~108,000 items). The scan is **partitioned by
  year** (`AND dc.date.issued:YYYY`) and paged at 100/request to stay under
  DSpace's deep-pagination ceiling.
- **Full text** — `embed=bundles/bitstreams` returns each item's files inline;
  the plain-text bitstream from the `TEXT` bundle (or an `ORIGINAL` `.txt`
  sibling) is downloaded directly. Modern acts (~1990s–2010) are born-digital
  and clean; older volumes are readable OCR of print. **No local PDF/OCR step.**

## Coverage

- ~108k Acts and Resolves, **1692–2010**.
- `_type`: `legislation`; jurisdiction `US-MA`.

## Usage

```bash
python3 bootstrap.py test-api            # connectivity / extraction test
python3 bootstrap.py bootstrap --sample  # 12 sample records (newest first)
python3 bootstrap.py bootstrap           # full pull (all years)
python3 bootstrap.py bootstrap-fast      # alias for full pull (VPS wrapper)
```

## License

[Public domain — US state statutes (edicts of government)](https://www.law.cornell.edu/uscode/text/17/105) — Massachusetts session laws (Acts & Resolves) are statutes, i.e. edicts of government, in the public domain (*Banks v. Manchester*; *Georgia v. Public.Resource.Org*). Digitized and served openly by the State Library of Massachusetts. No attribution required; commercial use permitted.
