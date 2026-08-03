# UK/ValuationTribunalWales — Valuation Tribunal for Wales (Tribiwnlys Prisio Cymru)

Full-text decisions of the **Valuation Tribunal for Wales (VTW)**, the independent
statutory tribunal for Wales that resolves disputes about property valuation for:

- **non-domestic (business) rating** — entries in the rating list,
- **council tax** — banding and liability, and
- **land drainage rates**.

VTW's written, reasoned decisions are binding and appealable to the Upper Tribunal
(Lands Chamber) or by way of case stated to the High Court — i.e. adjudicative
**case law** for the **GB-WLS (Wales)** jurisdiction. These decisions are **not**
covered by `UK/CaseLaw` (England & Wales superior courts + reserved UK tribunals,
indexed via the National Archives Find Case Law service), and VTW is a distinct
body from the separate `UK/ValuationTribunalEngland` (VTE).

## Coverage

- **~700+ full-text decisions**, 2018–present. The public portal only exposes
  decision documents from 2018 onward.
- Language: English (bilingual body; a minority of decisions are also/only
  published in Welsh — the scraper prefers the English document and falls back to
  Welsh where that is the only one available, tagging `language`).

## Access

`https://valuationtribunalwales.net/` is a public search portal backed by a single
JSON-over-POST web service (there is **no** server-rendered HTML listing; the
portal renders XHR responses client-side). The scraper drives that same service
directly — no browser automation, no auth:

1. **Decisions listing** — `POST /WebServer/index.php` with
   `request={"appeals":{"what":"html","query":{...}},"page":{...}}`, filtered on
   the decision-date window (`cri-app-dec-typ="between"` +
   `cri-app-dec-fro`/`cri-app-dec-too`), concluded appeals with a decision
   document (`cri-app-con-val=1`, `cri-app-dtx-val=1`). Each returned row carries
   `onclick='appeal(<internal id>)'` plus list metadata.
2. **Appeal detail** — `POST /WebServer/index.php` with
   `request={"appeal":{"query":{"id":<id>},"what":"html","grp":"app"},"page":{...}}`.
   The English decision-text cell wires `onclick='decisiontext(<doc>,<lan>)'`.
3. **Decision PDF** — `GET /PDFServer/?doc=<doc>&lan=<lan>` (lan `2` = English,
   `1` = Welsh) — a born-digital PDF with a clean text layer, extracted with
   PyMuPDF (shared pdfplumber/pypdf fallback). No OCR.

The scraper windows the listing by calendar year (2018 → current) to stay within
the service's response limits, then fetches the detail + PDF per appeal.

## Usage

```bash
python bootstrap.py test                # Quick connectivity + extraction check
python bootstrap.py bootstrap --sample  # 15 sample records for validation
python bootstrap.py bootstrap           # Full pull
python bootstrap.py bootstrap-fast      # Full pull (runner alias)
python bootstrap.py update              # Incremental (current + previous year)
```

## Record shape

Each normalized record is `_type: case_law`, `jurisdiction: GB-WLS`, with the full
decision `text`, `title` (reference + property), `date` (decision date), and
metadata: `case_ref`, `case_number`, `appeal_type`, `category` (Non-domestic
rating / Council tax / Land drainage rate), `property`, `billing_authority`,
`list_year`, `valuation_office`, `status`, `tribunal_dates`.

## License

[Open Government Licence v3.0](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/) — decisions of a statutory public tribunal funded by the Welsh Government; public-sector information / Crown copyright material, re-usable free of charge (attribution required; reproduce accurately, not in a misleading context). Commercial use permitted.
