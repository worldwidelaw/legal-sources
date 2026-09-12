# US/TX-DWC-AppealsPanel — Texas DWC Workers' Compensation Appeals Panel Decisions

Decisions of the **Appeals Panel** of the Texas Department of Insurance,
Division of Workers' Compensation (DWC).

The Appeals Panel reviews the decisions of administrative law judges (called
hearing officers before 2020) in contested case hearings under the Texas
Workers' Compensation Act, Tex. Lab. Code Ann. § 401.001 et seq. (the 1989
Act). Its decisions are Texas's workers' compensation case law and are cited by
appeal number — *APD 220175-s*. Decisions the panel designates **significant**
carry an `-s` suffix.

- **Type:** `case_law`
- **Coverage:** ~21,550 decisions, 1991 – present
- **Format:** born-digital PDFs (clean text at every era, including 1991)
- **Auth:** none

## Access

`https://www.tdi.texas.gov/appeals/` is **403** — there is no directory
listing. The enumeration comes from the public
[Appeals Panel Decisions search tool](https://wwwapps.tdi.texas.gov/inter/perlroot/wc/appeals/index.html),
a DataTables front end whose own AJAX source is an anonymous SAS broker
endpoint:

```
https://wwwapps.tdi.texas.gov/inter/perlroot/sasweb9/cgi-bin/broker.exe
    ?_service=wcExt&_program=progExt.Appeal_API.sas&opt=GET
```

One call returns the **entire** index (~5 MB JSON, no paging) with a row per
decision:

```json
{"orderDesc": "Total Remand", "yr": "2026", "appealNum": "260955",
 "URL": "https://www.tdi.texas.gov/appeals/2026cases/260955r.pdf",
 "Issues": "Extent of Injury, Dispute of DD MMI Date, Dispute of DD IR",
 "decisionDate": "07/13/2026", "sigCase": "", "doc": "T"}
```

The decision PDF is then fetched from `URL` and extracted with the shared
`common.pdf_extract` helper.

### Deduplication

The raw index holds 21,768 rows but only **21,553 distinct decisions**. A
designated-significant decision is published twice — once from its year folder
and once, byte-identical, from `/appeals/sig_cases/` — and a few decisions are
served from two year folders when the filing year and the publication year
differ (`221683r.pdf` under both `/2022cases/` and `/2023cases/`). `index()`
collapses rows on the appeal number, prefers the year folder over `sig_cases/`
and the folder matching the row's own year, and ORs the significance flag
across the rows it drops, so `significant` survives on the surviving row.

### Committed index snapshot

`wwwapps.tdi.texas.gov` does not answer datacenter vantages ([#1402]), and the
broker is the only enumeration there is — so a fleet run from a blocked IP could
not reach a single PDF even though `www.tdi.texas.gov`, which serves them, is a
different host. The index is therefore also carried here, gzipped, as
`index_snapshot.json.gz` (~250 KB, the seven fields `normalize` reads) and used
whenever the live call fails. A blocked run then writes the whole corpus as of
the snapshot's `captured` date rather than nothing, and says so loudly in the
log; only decisions published since the capture are missing.

Re-capture it from a vantage the broker answers:

```bash
python bootstrap.py refresh-index
```

If neither the broker nor the snapshot yields rows, `index()` still raises —
there is no path to the PDFs without an enumeration, so that must not degrade
into a zero-record run.

[#1402]: https://github.com/ZachLaik/LegalDataHunter/issues/1402

### Crawl shape

The index is resolved once per run and cached on the instance. The crawl is then
partitioned by year, oldest first, and completed years are checkpointed to
`data/tx_dwc_apd_checkpoint.json` — a killed run resumes instead of
re-downloading the years it already wrote.

## Usage

```bash
python bootstrap.py test-api          # index reachable + one PDF extracts
python bootstrap.py bootstrap --sample
python bootstrap.py bootstrap         # full pull
python bootstrap.py bootstrap-fast    # full pull (VPS wrapper alias)
python bootstrap.py refresh-index     # re-capture index_snapshot.json.gz
```

## Record shape

| Field | Notes |
|---|---|
| `_id` | `TX-DWC-APD-{appealNum}` |
| `title` | `Appeals Panel Decision No. 260955 — Total Remand` |
| `text` | full decision text (samples run ~2,000–14,000 chars) |
| `date` | index file date; falls back to the `FILED <date>` caption, then the contested-case-hearing date |
| `docket_number` / `citation` | appeal number / `APD {appeal number}` |
| `disposition` | index `orderDesc` — *Decision Entered*, *Total Remand*, *Partial Remand*, … |
| `issues` | index issue list, split on commas |
| `significant` | designated-significant decision (`-s`) |

## License

[Public domain — 17 U.S.C. § 105](https://www.law.cornell.edu/uscode/text/17/105) —
US state government edict; the decisions of a state adjudicative body are not
subject to copyright. Commercial use permitted, no attribution required.
