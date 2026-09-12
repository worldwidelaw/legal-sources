# BN/AGC-Legislation — Brunei Attorney General's Chambers Legislation (archive tree)

**Source:** [https://www.agc.gov.bn/AGC%20Site%20Pages/Laws%20of%20Brunei.aspx](https://www.agc.gov.bn/AGC%20Site%20Pages/Laws%20of%20Brunei.aspx)
**Data types:** legislation

## Scope

The AGC "Laws of Brunei" index page used to carry a column of PDF links; in mid-2026
the AGC stripped that column, leaving a plain chapter-number/title table with no
anchors at all (issue #1354). The PDFs themselves are still served from
`/AGC Images/` — only the index disappeared.

`BN/AGCLaws` rebuilds the *index-page* corpus (top-level `LAWS/ACT_PDF/`, `LOB/PDF/`,
`LOB/pdf/`, `LOB/PDF (EN)/`) from the newest Wayback capture that still has the links.
**This source covers the rest of the AGC legislation tree, which the index page never
linked** — 708 PDFs:

| Directory | Docs | Contents |
|---|---:|---|
| `LAWS/ACT_PDF/{A..Z}/` | 554 | Alphabetical revised editions, incl. historical 1984/2001/2002 revisions and post-repeal replacement notices |
| `LAWS/BLUV/` | 60 | Orders in the Brunei Laws Updating Volume |
| `LAWS/ENACTMENT/{year}/` | 22 | Historical enactments, 1908–1975 |
| `LOB/Order/**` | 45 | Subsidiary orders, filed by title |
| `LOB/chapter 157 (Statute\|Regulation)/` | 16 | UBD statutes and regulations under Cap. 157 |
| `LOB/Order PDF (EN)/` | 6 | Subsidiary orders (English) |
| `LOB/cons_doc/` | 5 | Consolidated constitution volumes |

Excluded: the gazette tree (`Gazette_PDF`, `GAZETTE NOTIFICATION` — covered by
`BN/AGC-GazetteII`) and the Malay-only directories (`Peng_PDF`, `(BM)`).

## How it works

1. **Index** — the directories are not browsable and no live page links them, so the
   PDF inventory is enumerated from the Internet Archive CDX index over
   `agc.gov.bn/AGC Images/LAWS*` and `.../LOB*`, then filtered to the directories
   above. An empty index raises rather than reporting a silent success.
2. **Fetch** — each PDF is downloaded **live** from `agc.gov.bn`. Only when the live
   host 404s a path that used to exist (e.g. `ACT_PDF/A/CHAPTER 113.pdf`) does it fall
   back to replaying the newest archived copy via `/web/{ts}id_/`.
3. **Text** — extracted with the shared `common.pdf_extract`. Titles are recovered
   from the PDF cover block (`CHAPTER 31` / `ANTIQUITIES AND TREASURE TROVE ACT`)
   because the filenames in the alphabetical tree are bare chapter numbers.

Some older documents (notably `LOB/cons_doc`, `chapter 157`, and a few 1980s
revisions) are scanned images and need OCR; they are skipped where OCR is
unavailable and picked up where it is.

## Usage

```bash
python bootstrap.py test              # build the index, report size + live reachability
python bootstrap.py bootstrap         # 15-record sample into sample/
python bootstrap.py bootstrap --full  # full pull to data/records.jsonl
python bootstrap.py bootstrap-fast    # full pull, concurrent extraction
```

## License

Open government data — [Laws of Brunei, Attorney General's Chambers](https://www.agc.gov.bn/AGC%20Site%20Pages/Laws%20of%20Brunei.aspx). Brunei government legislation published for public access; no attribution requirement stated.
