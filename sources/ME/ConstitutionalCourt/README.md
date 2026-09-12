# Montenegro Constitutional Court (Ustavni sud Crne Gore)

Case law from the Constitutional Court of Montenegro.

## Source Information

- **Website**: http://www.ustavnisud.me
- **Data Type**: Case law (constitutional complaints, constitutional reviews)
- **Coverage**: 1964 to present — ~20,100 indexed decisions, of which ~80% (~16,100)
  yield real extractable full text
- **Language**: Serbian / Montenegrin (sr)
- **License**: Public

## Data Access

Two endpoints are used together.

### 1. Index — `/ustavnisud/upit.php`

DataTables server-side API.

- **Method**: POST
- **Format**: JSON
- **Pagination**: `start` / `length` / `draw`
- **Ordering**: we page **ascending by date**, so newly published decisions append at
  the tail and the offset checkpoint stays valid across restarts.

`mod_security` answers **406 Not Acceptable** unless the request looks like the
site's own jQuery XHR — send `Accept: application/json, text/javascript, */*; q=0.01`,
`X-Requested-With: XMLHttpRequest`, a browser `User-Agent` and `Referer: arhiva.php`.

### 2. Full text — `/ustavnisud/obrada_fajlovi.php?iddok=<id>`

Per-decision attachment endpoint (accepts GET as well as POST). Returns
`{"linkovi": [{"link": "<a href='./webfolder/...'>", "naziv_fajla": ..., "korisnicki_naziv": ..., "tip": ...}]}`.

The `webfolder/` link is a **freshly generated temp export** (its filename embeds the
current timestamp), so it is not a stable permalink; `obrada_fajlovi.php?iddok=N` is
the stable per-decision address and is what we store in `url`. The site exposes no
other per-document permalink — `arhiva.php` ignores query parameters.

Attachment formats seen, by frequency: `.docx` ≈ 46%, legacy binary `.doc` ≈ 44%,
`.pdf` ≈ 9%.

## Do not use `sadrzaj_fajlova` as the text (issue #1254)

`upit.php` exposes a `sadrzaj_fajlova` column that *looks* like full text but is the
publisher's **search-index copy**: every diacritic, every punctuation mark and every
newline has been deleted in their own database.

Proof, using the API's own `sadrzaj` search filter:

| query | hits |
|---|---|
| `sadrzaj=Drašković` (correct spelling) | 0 |
| `sadrzaj=Drakovi` (stripped spelling) | 1042 |
| `sadrzaj=sud, u` (with a comma) | 0 |

So the stripping is upstream, not in our transport, and it is **not recoverable** from
that field. It also corrupts identifiers embedded in the body — `U-I br. 116/26`
becomes `UI br 11626`. The attachments carry correct UTF-8 with punctuation and line
breaks intact.

Decisions with no readable attachment are **skipped**, not emitted with the corrupted
index text.

## Text extraction

| Format | Method |
|---|---|
| `.docx` | stdlib `zipfile` + `word/document.xml` |
| `.doc` (OLE) | FIB → `Clx` → piece table; compressed pieces decoded as **cp1250**, others as UTF-16LE. This is what preserves č ć ž š đ — a naive byte scrape mangles them. Requires `olefile`. |
| `.pdf` | PyMuPDF → pdfplumber (with per-page cache flush) → pypdf |

Whitespace normalisation is deliberately conservative: it collapses runs of spaces and
blank lines only, and never touches non-ASCII characters, punctuation or line
structure.

## Known gap: pre-2005 scans

Decisions from roughly 1964–2004 are attached as **scanned image PDFs** (4 images per
page, 0 extractable characters). They are skipped. Recovering them needs OCR.

## Fields

| Field | Description |
|-------|-------------|
| `text` | Full decision text extracted from the attachment |
| `url` | `obrada_fajlovi.php?iddok=<id>` — per-decision document endpoint |
| `case_number` | `djelovodni_broj`, e.g. `U-III br.383/25` |
| `case_type` / `case_year` | Parsed from the case number |
| `document_type` | `vrsta_dokumenta` — procedure type |
| `date` / `session_date` | Decision date / session date (ISO 8601) |
| `challenged_act` | `osporeni_akt` — act under review |
| `keywords` | `kljucne_rijeci_tagovi` |
| `constitutional_articles` | Montenegrin Constitution articles cited |
| `convention_articles` | ECHR articles cited |
| `applicant` | `komitent` |
| `document_title` | Publisher's own file label, e.g. `Rješenje U-II br. 14-26 - odbacuje se` |
| `document_files` | Attachment descriptors (name, type, url) |
| `internal_id` | `iddok` |

## Case Types

| Code | Description |
|------|-------------|
| U-I | Constitutional review of laws |
| U-II | Constitutional review of regulations |
| U-III | Constitutional complaints |
| U-IV | Competence disputes |
| U-V | Conflicts of jurisdiction |
| U-VI | Electoral disputes |
| U-VII | Prohibition of political parties |
| U-VIII | Other procedures |

## Usage

```bash
# Connectivity + extraction smoke test
python3 bootstrap.py test

# Fetch sample data (15 records, newest first)
python3 bootstrap.py bootstrap --sample

# Fetch the whole corpus -> data/records.jsonl (resumable)
python3 bootstrap.py bootstrap --full
python3 bootstrap.py bootstrap-fast --full   # alias the fleet uses

# Fetch recent decisions
python3 bootstrap.py updates --since 2026-01-01
```

The full crawl writes an offset checkpoint to `data/checkpoint.json`, so an
interrupted or re-launched run advances monotonically instead of re-walking from the
start. Pass `--no-resume` to ignore it.

## License

Open government data — publicly accessible court decisions.

## Notes

- Full text is 4.7K–103K characters per decision (~37K average in the sample).
- Rate limiting: 1.0 second delay between decisions.
- `olefile` is required for the legacy `.doc` majority; without it those decisions
  are skipped.
