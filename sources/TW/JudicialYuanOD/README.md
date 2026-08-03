# Judicial Yuan Open Data (Taiwan court judgments)

**Source:** [https://opendata.judicial.gov.tw/](https://opendata.judicial.gov.tw/)
**Country:** TW
**Data types:** case_law
**Status:** Blocked — code complete, awaiting durable account credentials

## What this is

Full-text judgments (裁判書) of all Taiwan courts published by the Judicial
Yuan, via two complementary APIs. Both authenticate at **runtime from durable
account credentials** (`TW_JUDICIAL_ACCOUNT` / `TW_JUDICIAL_PASSWORD`) — no
token is ever stored or committed.

### 1. Historical monthly archives (bulk backfill / recovery)

`opendata.judicial.gov.tw`

- `POST /api/MemberTokens { memberAccount, pwd }` → JWT (used as `Bearer`).
  *Verified live:* wrong credentials return `{"succeeded":false,"message":"帳號或密碼錯誤!"}`.
- `GET /api/Datasets?keyword=裁判書` → paginated catalog. Monthly archives are
  datasets titled `{YYYYMM}裁判書`, some with a `--(YYYYMMDDUpdate)` suffix; the
  scraper keeps the **newest version per month** (`select_newest_per_month`).
- `GET /api/FilesetLists/{fileSetId}/file` → the archive (RAR of judgment JSON).
  *Verified live:* this route serves public files directly; the judgment
  archives are gated behind the MemberTokens `Bearer` (anonymous requests get a
  "not found" gate).

Archives span 1996-present (~360 months). Each judgment JSON uses the published
schema (`JID`, `JYEAR`, `JCASE`, `JNO`, `JDATE`, `JTITLE`, `JFULL`).

### 2. Delta / full-text API (incremental, night-only)

`data.judicial.gov.tw`

- `POST /jdg/api/Auth { user, password }` → 6-hour token.
- `GET /jdg/api/JList` → recently added/corrected JIDs (limited window).
- `GET /jdg/api/JDoc?j={JID}` → per-judgment full text + deletion/non-public flags.

**Service window:** the delta API only answers **00:00–06:00 Asia/Taipei**.
*Verified live:* outside the window every call returns
`{"error":"目前非本 API 服務時間。"}` ("not currently API service time"). The
scraper detects this (`in_service_window`) and `update` exits cleanly when the
window is closed. Because `JList` only exposes a short historical window, the
monthly archives are the recovery/backfill path.

Deletion / non-public signals from `JDoc` cause the affected JID to be withheld
(`normalize` returns `None`).

## Configuration

```bash
export TW_JUDICIAL_ACCOUNT="..."
export TW_JUDICIAL_PASSWORD="..."
```

Register at https://opendata.judicial.gov.tw/. Membership requires an email
reconfirmation roughly every three months to stay active.

## Usage

```bash
python bootstrap.py test               # auth + list monthly archives (needs creds)
python bootstrap.py bootstrap --sample # 15 sample judgments
python bootstrap.py bootstrap --full   # full historical pull (RAR archives)
python bootstrap.py bootstrap-fast     # concurrent historical pull
python bootstrap.py update             # nightly delta (window-gated; no-op by day)
python test_scraper.py                 # 18 offline regression tests
```

`test` exits `2` when credentials are absent, `1` on auth failure.

## Dependencies

RAR extraction (`iter_judgments_from_rar`) uses `rarfile`, which needs an
`unrar` / `bsdtar` / `unar` backend on `PATH`. It is imported lazily, so the
module and tests run without it.

## Tests

`test_scraper.py` (18 tests, no network / no credentials): MemberTokens request
shape + token extraction + login-failure handling; service-window detection and
the "not service time" error; newest-version-per-month selection; judgment JSON
normalization (incl. `JDATE` → ISO); deletion / non-public withholding; and RAR
member JSON iteration (single object / array / JSON-lines).

## Why still "blocked"

The scraper, both auth flows, catalog discovery, window handling, and
normalization are complete and unit-tested, but a live sample run needs a
Judicial Yuan account (I have none) — and the delta API only answers at night.
Once `TW_JUDICIAL_ACCOUNT` / `TW_JUDICIAL_PASSWORD` are provisioned, run
`bootstrap --full` and flip to `complete`.

## License

[Taiwan Open Government Data License v1](https://data.gov.tw/license) — CC BY 4.0
compatible; attribution required. Commercial use permitted.
